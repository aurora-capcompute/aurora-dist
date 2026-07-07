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

// fakeRuntime stubs the slice of the runtime the timer service reads and
// resolves through. It is mutable so a test can change state between reconciles.
type fakeRuntime struct {
	mu       sync.Mutex
	sessions map[string]aurora.SessionSnapshot
	tasks    map[string][]aurora.TaskSnapshot
	resolved chan string
}

func (f *fakeRuntime) ListSessions() []aurora.SessionSummary {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []aurora.SessionSummary
	for id, session := range f.sessions {
		summary := session.SessionSummary
		summary.ID = id
		out = append(out, summary)
	}
	return out
}

func (f *fakeRuntime) GetSession(id string) (aurora.SessionSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.sessions[id], nil
}

func (f *fakeRuntime) Tasks(processID string) ([]aurora.TaskSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.tasks[processID], nil
}

func (f *fakeRuntime) ResolveTask(taskID, token string, resolution aurora.Resolution) (aurora.TaskSnapshot, error) {
	select {
	case f.resolved <- taskID + "/" + string(resolution.Decision) + "/" + resolution.Actor:
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
		Syscall:         sys.Syscall{Abi: sys.ABIVersion, Name: sys.SyscallTimer, Args: args},
	}
}

func TestScheduleFiresElapsedTimerImmediately(t *testing.T) {
	fake := &fakeRuntime{resolved: make(chan string, 1)}
	service := New(fake, slog.Default())
	defer service.StopAll()

	// Created long ago: the fire time already passed, so it fires now.
	service.Schedule(timerTask("t1", "proc_1", time.Now().Add(-time.Hour), 1))
	select {
	case got := <-fake.resolved:
		if got != "t1/completed/timer" {
			t.Fatalf("resolved = %q", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("elapsed timer did not fire")
	}
}

func TestReconcileArmsAndDisarms(t *testing.T) {
	fake := &fakeRuntime{
		resolved: make(chan string, 1),
		sessions: map[string]aurora.SessionSnapshot{
			"ses_1": {Processes: []aurora.ProcessSnapshot{{ID: "proc_1", Status: aurora.ProcessWaitingTask}}},
		},
		tasks: map[string][]aurora.TaskSnapshot{
			"proc_1": {timerTask("t1", "proc_1", time.Now(), 3600)},
		},
	}
	service := New(fake, slog.Default())
	defer service.StopAll()

	// A pending timer task on a parked process is armed.
	service.Reconcile()
	if _, ok := service.FireAtFor("proc_1"); !ok {
		t.Fatal("reconcile did not arm the pending timer task")
	}

	// A non-timer task on a parked process is ignored.
	fake.mu.Lock()
	other := timerTask("t2", "proc_2", time.Now(), 3600)
	other.Syscall.Name = "net.http"
	fake.sessions["ses_2"] = aurora.SessionSnapshot{Processes: []aurora.ProcessSnapshot{{ID: "proc_2", Status: aurora.ProcessWaitingTask}}}
	fake.tasks["proc_2"] = []aurora.TaskSnapshot{other}
	fake.mu.Unlock()
	service.Reconcile()
	if _, ok := service.FireAtFor("proc_2"); ok {
		t.Fatal("a non-timer task was armed")
	}

	// Once the process finishes (task gone), reconcile disarms its timer.
	fake.mu.Lock()
	fake.tasks["proc_1"] = nil
	fake.sessions["ses_1"] = aurora.SessionSnapshot{Processes: []aurora.ProcessSnapshot{{ID: "proc_1", Status: aurora.ProcessCompleted}}}
	fake.mu.Unlock()
	service.Reconcile()
	if _, ok := service.FireAtFor("proc_1"); ok {
		t.Fatal("reconcile kept a timer for a finished process")
	}
}

func TestScheduleIsIdempotentAndSkipsNonPending(t *testing.T) {
	service := New(&fakeRuntime{resolved: make(chan string, 1)}, slog.Default())
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
