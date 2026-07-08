// Package timers fires durable sys.timer tasks. It is a distribution-owned
// service — deliberately not a terminal concern: a timer must fire whether or
// not any client is attached. The service reconciles its armed in-process
// timers against runtime state on a ticker (and once at boot): every pending
// sys.timer task on a parked process gets an armed timer; when it elapses the
// task is resolved with Completed, which resumes the waiting process. Fire
// times are derived from the persisted task (created_at + duration), so they
// are restart-safe — boot recovery re-arms pending timers and fires any that
// already elapsed. Reading task state directly, rather than observing an event
// stream, is the seam here: the resolution token needed to fire lives on the
// task snapshot.
package timers

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/aurora-capcompute/aurora-capcompute/aurora"
)

// Runtime is the slice of the runtime the service reads and resolves through.
// aurora.Runtime satisfies it.
type Runtime interface {
	ListSessions() []aurora.SessionSummary
	GetSession(sessionID string) (aurora.SessionSnapshot, error)
	Tasks(processID string) ([]aurora.TaskSnapshot, error)
	ResolveTask(taskID, token string, resolution aurora.Resolution) (aurora.TaskSnapshot, error)
}

type Service struct {
	runtime Runtime
	logger  *slog.Logger
	now     func() time.Time

	mu     sync.Mutex
	timers map[string]*time.Timer
}

func New(runtime Runtime, logger *slog.Logger) *Service {
	if logger == nil {
		logger = slog.Default()
	}
	return &Service{
		runtime: runtime,
		logger:  logger,
		now:     time.Now,
		timers:  make(map[string]*time.Timer),
	}
}

// Start runs the reconcile loop: it reconciles once immediately (boot recovery)
// and then every interval until ctx is cancelled. Firing itself is exact — an
// in-process time.AfterFunc per armed timer — so the interval only bounds how
// quickly a newly created or newly resolved timer is discovered, never the fire
// time (which is absolute: created_at + duration).
func (s *Service) Start(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = time.Second
	}
	s.Reconcile()
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.Reconcile()
			}
		}
	}()
}

// Reconcile brings the armed timer set in line with runtime state: it arms a
// timer for every pending sys.timer task on a parked process and disarms any
// armed timer whose task is no longer pending — resolved, or its process
// finished. It is idempotent and is the sole arming path, safe to call at boot
// and on the ticker.
func (s *Service) Reconcile() {
	valid := map[string]aurora.TaskSnapshot{}
	for _, summary := range s.runtime.ListSessions() {
		session, err := s.runtime.GetSession(summary.ID)
		if err != nil {
			continue
		}
		for _, process := range session.Processes {
			if process.Status != aurora.ProcessWaitingTask && process.Status != aurora.ProcessYielded {
				continue
			}
			tasks, err := s.runtime.Tasks(process.ID)
			if err != nil {
				s.logger.Warn("timer reconcile: list tasks", "process_id", process.ID, "error", err)
				continue
			}
			for _, task := range tasks {
				if task.State == aurora.TaskStatePending && IsTimerTask(task) {
					valid[task.ID] = task
				}
			}
		}
	}
	for _, task := range valid {
		s.Schedule(task)
	}
	s.mu.Lock()
	var stale []string
	for id := range s.timers {
		if _, ok := valid[id]; !ok {
			stale = append(stale, id)
		}
	}
	s.mu.Unlock()
	for _, id := range stale {
		s.Cancel(id)
	}
}

// Schedule arms a timer for the task. It is idempotent: arming an already-
// armed task is a no-op, so it is safe to call on every reconcile pass.
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
	taskID, token := task.ID, task.ResolutionToken
	s.timers[task.ID] = time.AfterFunc(delay, func() { s.fire(taskID, token, label) })
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
	if _, err := s.runtime.ResolveTask(taskID, token, aurora.Resolution{
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
		entry.Stop()
		delete(s.timers, taskID)
	}
}

// StopAll stops every armed timer. Called on shutdown.
func (s *Service) StopAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, entry := range s.timers {
		entry.Stop()
		delete(s.timers, id)
	}
}

// IsTimerTask reports whether the task is a sys.timer call.
func IsTimerTask(task aurora.TaskSnapshot) bool {
	return task.Syscall.Name == aurora.TimerSyscall
}

// FireAt derives the absolute fire time and label from a timer task. It
// returns false for any task that is not a well-formed timer.
func FireAt(task aurora.TaskSnapshot) (time.Time, string, bool) {
	if !IsTimerTask(task) {
		return time.Time{}, "", false
	}
	var request struct {
		DurationSeconds int64  `json:"duration_seconds"`
		Label           string `json:"label,omitempty"`
	}
	if err := json.Unmarshal(task.Syscall.Args, &request); err != nil || request.DurationSeconds <= 0 {
		return time.Time{}, "", false
	}
	fireAt := task.CreatedAt.Add(time.Duration(request.DurationSeconds) * time.Second)
	return fireAt, request.Label, true
}
