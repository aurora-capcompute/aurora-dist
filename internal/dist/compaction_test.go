package dist

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/aurora-capcompute/aurora-capcompute/aurora"
)

// fakeCompactRuntime stubs the one call the compaction loop makes.
type fakeCompactRuntime struct {
	aurora.Runtime
	mu    sync.Mutex
	calls int
}

func (f *fakeCompactRuntime) CompactSessions(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return nil
}

func (f *fakeCompactRuntime) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// The compaction loop ticks CompactSessions on its interval, stops with its
// context, and a negative interval disables it entirely.
func TestStartCompactionTicksAndDisables(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	ticking := &fakeCompactRuntime{}
	ctx, cancel := context.WithCancel(context.Background())
	d := &Dist{Runtime: ticking, logger: logger}
	d.startCompaction(ctx, 5*time.Millisecond)
	deadline := time.Now().Add(5 * time.Second)
	for ticking.count() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	if ticking.count() < 2 {
		t.Fatalf("compaction loop ticked %d times, want >= 2", ticking.count())
	}
	settled := ticking.count()
	time.Sleep(25 * time.Millisecond)
	if ticking.count() > settled+1 { // one in-flight tick may land after cancel
		t.Fatalf("compaction loop kept ticking after cancel: %d -> %d", settled, ticking.count())
	}

	disabled := &fakeCompactRuntime{}
	d2 := &Dist{Runtime: disabled, logger: logger}
	d2.startCompaction(context.Background(), -1)
	time.Sleep(25 * time.Millisecond)
	if disabled.count() != 0 {
		t.Fatalf("negative interval must disable compaction, got %d ticks", disabled.count())
	}
}
