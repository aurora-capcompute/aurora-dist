package timers

import (
	"encoding/json"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/aurora-capcompute/aurora-capcompute/aurora"
	"github.com/aurora-capcompute/capcompute/sys"
)

type resolverFunc struct {
	mu    sync.Mutex
	calls []string
	done  chan struct{}
}

func (r *resolverFunc) ResolveTask(taskID, token string, resolution aurora.Resolution) (aurora.TaskSnapshot, error) {
	r.mu.Lock()
	r.calls = append(r.calls, taskID+"/"+string(resolution.Decision)+"/"+resolution.Actor)
	r.mu.Unlock()
	select {
	case r.done <- struct{}{}:
	default:
	}
	return aurora.TaskSnapshot{}, nil
}

func timerTask(id, processID string, createdAt time.Time, seconds int) aurora.TaskSnapshot {
	args, _ := json.Marshal(map[string]any{"duration_seconds": seconds, "label": "nap"})
	return aurora.TaskSnapshot{
		ID:              id,
		ProcessID:       processID,
		State:           aurora.TaskStatePending,
		CreatedAt:       createdAt,
		ResolutionToken: "tok-" + id,
		Syscall:         sys.Syscall{Abi: sys.ABIVersion, Name: "timer.set", Args: args},
	}
}

func TestScheduleFiresElapsedTimerImmediately(t *testing.T) {
	resolver := &resolverFunc{done: make(chan struct{}, 1)}
	service := New(resolver, slog.Default())
	defer service.StopAll()

	// Created long ago: the fire time already passed, so recovery fires now.
	service.Schedule(timerTask("t1", "proc_1", time.Now().Add(-time.Hour), 1))
	select {
	case <-resolver.done:
	case <-time.After(2 * time.Second):
		t.Fatal("elapsed timer did not fire")
	}
	resolver.mu.Lock()
	defer resolver.mu.Unlock()
	if len(resolver.calls) != 1 || resolver.calls[0] != "t1/completed/timer" {
		t.Fatalf("calls = %v", resolver.calls)
	}
}

func TestObserveArmsAndCancels(t *testing.T) {
	resolver := &resolverFunc{done: make(chan struct{}, 1)}
	service := New(resolver, slog.Default())
	defer service.StopAll()

	task := timerTask("t1", "proc_1", time.Now(), 3600)
	service.Observe("task.created", task)
	if _, ok := service.FireAtFor("proc_1"); !ok {
		t.Fatal("timer was not armed from task.created")
	}
	// A non-timer task is ignored.
	other := task
	other.ID, other.Syscall.Name = "t2", "internet.read"
	service.Observe("task.created", other)

	// The process finishing cancels its pending timer.
	service.Observe("process.updated", aurora.ProcessSnapshot{ID: "proc_1", Status: aurora.ProcessStopped})
	if _, ok := service.FireAtFor("proc_1"); ok {
		t.Fatal("terminal process kept an armed timer")
	}
}

func TestObserveCancelsResolvedTask(t *testing.T) {
	service := New(&resolverFunc{done: make(chan struct{}, 1)}, slog.Default())
	defer service.StopAll()

	task := timerTask("t1", "proc_1", time.Now(), 3600)
	service.Schedule(task)
	resolved := task
	resolved.State = aurora.TaskStateCancelled
	service.Observe("task.updated", resolved)
	if _, ok := service.FireAtFor("proc_1"); ok {
		t.Fatal("resolved task kept an armed timer")
	}
}

func TestScheduleIsIdempotentAndSkipsNonPending(t *testing.T) {
	service := New(&resolverFunc{done: make(chan struct{}, 1)}, slog.Default())
	defer service.StopAll()

	task := timerTask("t1", "proc_1", time.Now(), 3600)
	service.Schedule(task)
	service.Schedule(task) // no double-arm
	service.mu.Lock()
	n := len(service.timers)
	service.mu.Unlock()
	if n != 1 {
		t.Fatalf("timers = %d, want 1", n)
	}
	done := task
	done.ID, done.State = "t2", aurora.TaskStateCompleted
	service.Schedule(done)
	if _, ok := service.timers["t2"]; ok {
		t.Fatal("non-pending task was armed")
	}
}
