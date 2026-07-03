// Package timers fires durable timer.set tasks. It is a distribution-owned
// service — deliberately not a terminal concern: a timer must fire whether or
// not any client is attached. When a timer task is created the service arms
// an in-process timer; when it elapses the task is resolved with Completed,
// which resumes the waiting process. Fire times are derived from the
// persisted task (created_at + duration), so they are restart-safe: boot
// recovery re-arms pending timers, firing immediately for any that already
// elapsed.
package timers

import (
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/aurora-capcompute/aurora-capcompute/aurora"
	"github.com/aurora-capcompute/aurora-dispatchers/timer"
)

// TaskResolver is the slice of the runtime the service needs. aurora.Runtime
// satisfies it.
type TaskResolver interface {
	ResolveTask(taskID, token string, resolution aurora.Resolution) (aurora.TaskSnapshot, error)
}

type Service struct {
	resolver TaskResolver
	logger   *slog.Logger
	now      func() time.Time

	mu     sync.Mutex
	timers map[string]*scheduledTimer
}

type scheduledTimer struct {
	timer     *time.Timer
	processID string
	fireAt    time.Time
}

func New(resolver TaskResolver, logger *slog.Logger) *Service {
	if logger == nil {
		logger = slog.Default()
	}
	return &Service{
		resolver: resolver,
		logger:   logger,
		now:      time.Now,
		timers:   make(map[string]*scheduledTimer),
	}
}

// Schedule arms a timer for the task. It is idempotent: arming an already-
// armed task is a no-op, so it is safe to call from both the task.created
// event and boot recovery.
func (s *Service) Schedule(task aurora.TaskSnapshot) {
	if task.State != aurora.TaskStatePending {
		return
	}
	fireAt, label, ok := FireAt(task)
	if !ok {
		s.logger.Warn("ignore malformed timer task", "task_id", task.ID)
		return
	}
	delay := fireAt.Sub(s.now())
	if delay < 0 {
		delay = 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.timers[task.ID]; exists {
		return
	}
	taskID, token, processID := task.ID, task.ResolutionToken, task.ProcessID
	s.timers[task.ID] = &scheduledTimer{
		timer:     time.AfterFunc(delay, func() { s.fire(taskID, token, label) }),
		processID: processID,
		fireAt:    fireAt,
	}
}

// FireAtFor returns the fire time of the timer currently armed for a process,
// if any.
func (s *Service) FireAtFor(processID string) (time.Time, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, entry := range s.timers {
		if entry.processID == processID {
			return entry.fireAt, true
		}
	}
	return time.Time{}, false
}

func (s *Service) fire(taskID, token, label string) {
	s.mu.Lock()
	delete(s.timers, taskID)
	s.mu.Unlock()

	data, err := json.Marshal(map[string]string{"status": "fired", "label": label})
	if err != nil {
		s.logger.Error("marshal timer result", "task_id", taskID, "error", err)
		return
	}
	if _, err := s.resolver.ResolveTask(taskID, token, aurora.Resolution{
		Decision: aurora.TaskStateCompleted, Data: data, Actor: "timer",
	}); err != nil {
		// The process may have been stopped or the task already resolved; that
		// is a benign no-op rather than an error worth surfacing.
		s.logger.Info("timer resolution skipped", "task_id", taskID, "error", err)
	}
}

// Cancel stops a single armed timer.
func (s *Service) Cancel(taskID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if entry, ok := s.timers[taskID]; ok {
		entry.timer.Stop()
		delete(s.timers, taskID)
	}
}

// CancelProcess stops every timer armed for a process. Called when a process
// reaches a terminal state so a pending timer does not fire against a
// finished process.
func (s *Service) CancelProcess(processID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, entry := range s.timers {
		if entry.processID == processID {
			entry.timer.Stop()
			delete(s.timers, id)
		}
	}
}

// StopAll stops every armed timer. Called on shutdown.
func (s *Service) StopAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, entry := range s.timers {
		entry.timer.Stop()
		delete(s.timers, id)
	}
}

// Observe reacts to one runtime session event: arming timers as their tasks
// are created and disarming them as their processes finish. The distribution
// feeds it every event from its tenant firehose.
func (s *Service) Observe(eventType string, data any) {
	switch eventType {
	case "task.created":
		if task, ok := data.(aurora.TaskSnapshot); ok && IsTimerTask(task) {
			s.Schedule(task)
		}
	case "task.updated":
		if task, ok := data.(aurora.TaskSnapshot); ok && task.State != aurora.TaskStatePending {
			s.Cancel(task.ID)
		}
	case "process.updated":
		if snapshot, ok := data.(aurora.ProcessSnapshot); ok {
			switch snapshot.Status {
			case aurora.ProcessCompleted, aurora.ProcessFailed, aurora.ProcessStopped, aurora.ProcessInterrupted:
				s.CancelProcess(snapshot.ID)
			}
		}
	}
}

// Recover re-arms pending timer tasks for every process still parked on one —
// the restart-safety path. Elapsed timers fire immediately.
func (s *Service) Recover(runtime aurora.Runtime) {
	for _, summary := range runtime.ListSessions() {
		session, err := runtime.GetSession(summary.ID)
		if err != nil {
			continue
		}
		for _, process := range session.Processes {
			if process.Status != aurora.ProcessWaitingTask && process.Status != aurora.ProcessYielded {
				continue
			}
			tasks, err := runtime.Tasks(process.ID)
			if err != nil {
				s.logger.Warn("timer recovery: list tasks", "process_id", process.ID, "error", err)
				continue
			}
			for _, task := range tasks {
				if task.State == aurora.TaskStatePending && IsTimerTask(task) {
					s.Schedule(task)
				}
			}
		}
	}
}

// IsTimerTask reports whether the task is a timer.set call.
func IsTimerTask(task aurora.TaskSnapshot) bool {
	return task.Syscall.Name == timer.Capability
}

// FireAt derives the absolute fire time and label from a timer task. It
// returns false for any task that is not a well-formed timer.
func FireAt(task aurora.TaskSnapshot) (time.Time, string, bool) {
	if !IsTimerTask(task) {
		return time.Time{}, "", false
	}
	var request timer.SetRequest
	if err := json.Unmarshal(task.Syscall.Args, &request); err != nil || request.DurationSeconds <= 0 {
		return time.Time{}, "", false
	}
	fireAt := task.CreatedAt.Add(time.Duration(request.DurationSeconds) * time.Second)
	return fireAt, request.Label, true
}
