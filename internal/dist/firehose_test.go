package dist

import (
	"sync"
	"testing"
	"time"

	"github.com/aurora-capcompute/aurora-capcompute/aurora"
)

// fakeRuntime implements the two Runtime methods the firehose uses; the
// embedded interface panics loudly if anything else is called.
type fakeRuntime struct {
	aurora.Runtime
	mu       sync.Mutex
	sessions []aurora.SessionSummary
	subs     map[string]chan aurora.Event
}

func newFakeRuntime(sessionIDs ...string) *fakeRuntime {
	f := &fakeRuntime{subs: make(map[string]chan aurora.Event)}
	for _, id := range sessionIDs {
		f.sessions = append(f.sessions, aurora.SessionSummary{ID: id})
	}
	return f
}

func (f *fakeRuntime) ListSessions() []aurora.SessionSummary {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]aurora.SessionSummary(nil), f.sessions...)
}

func (f *fakeRuntime) Subscribe(sessionID string) (aurora.Event, <-chan aurora.Event, func(), error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	ch := make(chan aurora.Event, 16)
	f.subs[sessionID] = ch
	return aurora.Event{Type: "snapshot"}, ch, func() {}, nil
}

func (f *fakeRuntime) emit(sessionID string, event aurora.Event) {
	f.mu.Lock()
	ch := f.subs[sessionID]
	f.mu.Unlock()
	ch <- event
}

func recv(t *testing.T, ch <-chan Frame) Frame {
	t.Helper()
	select {
	case frame := <-ch:
		return frame
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for a frame")
		return Frame{}
	}
}

func TestFirehoseMergesSessionsAndStampsSequence(t *testing.T) {
	runtime := newFakeRuntime("ses_a", "ses_b")
	f := newFirehose(runtime, 16)
	if err := f.start(); err != nil {
		t.Fatal(err)
	}
	defer f.close()

	replay, snapshot, live, cancel, err := f.subscribe(0)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	if len(replay) != 0 || len(snapshot) != 2 {
		t.Fatalf("fresh subscribe: replay=%d snapshot=%d", len(replay), len(snapshot))
	}

	runtime.emit("ses_a", aurora.Event{Type: "process.updated", Data: "a1"})
	runtime.emit("ses_b", aurora.Event{Type: "task.created", Data: "b1"})

	first, second := recv(t, live), recv(t, live)
	if first.Seq != 1 || second.Seq != 2 {
		t.Fatalf("sequences = %d, %d", first.Seq, second.Seq)
	}
	if first.SessionID != "ses_a" || second.SessionID != "ses_b" {
		t.Fatalf("session stamps = %s, %s", first.SessionID, second.SessionID)
	}
}

func TestFirehoseResumeFromRingOrSnapshot(t *testing.T) {
	runtime := newFakeRuntime("ses_a")
	f := newFirehose(runtime, 4)
	if err := f.start(); err != nil {
		t.Fatal(err)
	}
	defer f.close()

	for i := 0; i < 6; i++ {
		f.publish(Frame{SessionID: "ses_a", Type: "process.updated"})
	}
	// Ring holds seqs 3..6. A cursor inside it replays the gap, no snapshot.
	replay, snapshot, _, cancel, err := f.subscribe(4)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	if snapshot != nil || len(replay) != 2 || replay[0].Seq != 5 || replay[1].Seq != 6 {
		t.Fatalf("in-window resume: snapshot=%v replay=%+v", snapshot, replay)
	}
	// A cursor that scrolled out re-syncs via snapshot.
	replay, snapshot, _, cancel, err = f.subscribe(1)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	if len(replay) != 0 || len(snapshot) != 1 {
		t.Fatalf("out-of-window resume: replay=%d snapshot=%d", len(replay), len(snapshot))
	}
}

func TestFirehoseSessionCreatedAnnouncesAndWatches(t *testing.T) {
	runtime := newFakeRuntime()
	f := newFirehose(runtime, 16)
	defer f.close()

	_, _, live, cancel, err := f.subscribe(0)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()

	runtime.mu.Lock()
	runtime.sessions = append(runtime.sessions, aurora.SessionSummary{ID: "ses_new"})
	runtime.mu.Unlock()
	if err := f.sessionCreated(aurora.SessionSnapshot{SessionSummary: aurora.SessionSummary{ID: "ses_new"}}); err != nil {
		t.Fatal(err)
	}
	if frame := recv(t, live); frame.Type != "session.created" || frame.SessionID != "ses_new" {
		t.Fatalf("frame = %+v", frame)
	}
	// The new session is watched: its events flow.
	runtime.emit("ses_new", aurora.Event{Type: "progress", Data: "p"})
	if frame := recv(t, live); frame.Type != "progress" {
		t.Fatalf("frame = %+v", frame)
	}
}

func TestFirehoseTapObservesEveryFrame(t *testing.T) {
	runtime := newFakeRuntime("ses_a")
	f := newFirehose(runtime, 16)
	var mu sync.Mutex
	var seen []string
	f.tap = func(frame Frame) {
		mu.Lock()
		seen = append(seen, frame.Type)
		mu.Unlock()
	}
	if err := f.start(); err != nil {
		t.Fatal(err)
	}
	defer f.close()

	runtime.emit("ses_a", aurora.Event{Type: "task.created"})
	deadline := time.Now().Add(2 * time.Second)
	for {
		mu.Lock()
		n := len(seen)
		mu.Unlock()
		if n == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("tap never observed the frame")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
