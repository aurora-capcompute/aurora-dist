package dist

import (
	"sort"
	"sync"

	"github.com/aurora-capcompute/aurora-capcompute/aurora"
)

// Frame is one tenant-firehose event: a runtime session event stamped with
// the session it came from and a monotonically increasing sequence. The
// sequence is the resume cursor — an SSE client reconnects with the last seq
// it saw and replays the gap from the ring, or re-syncs from a fresh session
// snapshot when the gap has scrolled out.
type Frame struct {
	Seq       uint64 `json:"seq"`
	SessionID string `json:"session_id"`
	Type      string `json:"type"`
	Data      any    `json:"data"`
}

// firehose is the distribution's tenant-scoped event bus. The runtime only
// exposes per-session subscriptions; connectors need one stream for the whole
// tenant, so the dist subscribes to every session (existing ones at boot, new
// ones as its own API creates them — the API is the single way in) and fans
// the merged stream out with at-least-once delivery: a bounded replay ring
// plus snapshot-on-reconnect when the ring no longer covers the client's
// cursor. A subscriber that cannot keep up is disconnected rather than
// silently skipped — reconnecting re-syncs it.
type firehose struct {
	runtime aurora.Runtime
	ringMax int
	// tap, when set before start, observes every frame synchronously —
	// the in-process consumer seam (the timer service). Unlike subscribers
	// it is never disconnected for lag.
	tap func(Frame)

	mu      sync.Mutex
	seq     uint64
	ring    []Frame
	subs    map[uint64]chan Frame
	nextSub uint64
	watches map[string]func()
	closed  bool
}

func newFirehose(runtime aurora.Runtime, ringMax int) *firehose {
	if ringMax <= 0 {
		ringMax = 8192
	}
	return &firehose{
		runtime: runtime,
		ringMax: ringMax,
		subs:    make(map[uint64]chan Frame),
		watches: make(map[string]func()),
	}
}

// start watches every session already known to the runtime (restore rebuilt
// them before the firehose exists, so boot-time coverage is complete).
func (f *firehose) start() error {
	for _, session := range f.runtime.ListSessions() {
		if err := f.watch(session.ID); err != nil {
			return err
		}
	}
	return nil
}

// sessionCreated announces a session created through the dist API and begins
// watching it. The synthetic session.created frame is what lets a connector
// discover new sessions from the firehose alone.
func (f *firehose) sessionCreated(snapshot aurora.SessionSnapshot) error {
	f.publish(Frame{SessionID: snapshot.ID, Type: "session.created", Data: snapshot})
	return f.watch(snapshot.ID)
}

// watch subscribes to one session and pumps its events into the bus. The
// per-session snapshot event the runtime sends on subscribe is dropped:
// firehose clients snapshot at connect time instead.
func (f *firehose) watch(sessionID string) error {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return nil
	}
	if _, ok := f.watches[sessionID]; ok {
		f.mu.Unlock()
		return nil
	}
	f.mu.Unlock()

	_, events, cancel, err := f.runtime.Subscribe(sessionID)
	if err != nil {
		return err
	}

	f.mu.Lock()
	if f.closed || f.watches[sessionID] != nil {
		f.mu.Unlock()
		cancel()
		return nil
	}
	f.watches[sessionID] = cancel
	f.mu.Unlock()

	go func() {
		for event := range events {
			f.publish(Frame{SessionID: sessionID, Type: event.Type, Data: event.Data})
		}
	}()
	return nil
}

func (f *firehose) publish(frame Frame) {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return
	}
	f.seq++
	frame.Seq = f.seq
	f.ring = append(f.ring, frame)
	if len(f.ring) > f.ringMax {
		f.ring = f.ring[len(f.ring)-f.ringMax:]
	}
	for id, ch := range f.subs {
		select {
		case ch <- frame:
		default:
			// The subscriber lagged past its buffer. Disconnect it — the SSE
			// handler ends the response and the client re-syncs on reconnect —
			// instead of silently dropping frames mid-stream.
			close(ch)
			delete(f.subs, id)
		}
	}
	tap := f.tap
	f.mu.Unlock()
	if tap != nil {
		tap(frame)
	}
}

// subscribe attaches a firehose client. When the client's cursor (after) is
// still covered by the ring, the gap is returned for replay and no snapshot
// is needed; otherwise the caller gets the current session summaries as a
// re-sync snapshot. The live channel is registered before the replay/snapshot
// is computed, so nothing published in between is lost (duplicates are
// possible — delivery is at-least-once by design).
func (f *firehose) subscribe(after uint64) (replay []Frame, snapshot []aurora.SessionSummary, live <-chan Frame, cancel func(), err error) {
	f.mu.Lock()
	ch := make(chan Frame, 256)
	f.nextSub++
	id := f.nextSub
	f.subs[id] = ch

	covered := after > 0 && len(f.ring) > 0 && f.ring[0].Seq <= after+1 && after <= f.seq
	if after == f.seq && after > 0 {
		covered = true
	}
	if covered {
		idx := sort.Search(len(f.ring), func(i int) bool { return f.ring[i].Seq > after })
		replay = append([]Frame(nil), f.ring[idx:]...)
	}
	f.mu.Unlock()

	if !covered {
		snapshot = f.runtime.ListSessions()
		sort.Slice(snapshot, func(i, j int) bool { return snapshot[i].CreatedAt.Before(snapshot[j].CreatedAt) })
	}
	var once sync.Once
	cancel = func() {
		once.Do(func() {
			f.mu.Lock()
			if _, ok := f.subs[id]; ok {
				delete(f.subs, id)
				close(ch)
			}
			f.mu.Unlock()
		})
	}
	return replay, snapshot, ch, cancel, nil
}

func (f *firehose) close() {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return
	}
	f.closed = true
	watches := f.watches
	f.watches = map[string]func(){}
	for id, ch := range f.subs {
		close(ch)
		delete(f.subs, id)
	}
	f.mu.Unlock()
	for _, cancel := range watches {
		cancel()
	}
}
