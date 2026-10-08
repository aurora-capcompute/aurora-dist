package dist

import (
	"io"
	"log/slog"
	"sort"
	"testing"

	"github.com/aurora-capcompute/aurora-capcompute/aurora"
)

// fakeResumeRuntime stubs the read + retry surface resumeInterrupted uses.
type fakeResumeRuntime struct {
	aurora.Runtime
	sessions map[string]aurora.SessionSnapshot
	retried  []string
}

func (f *fakeResumeRuntime) ListSessions() []aurora.SessionSummary {
	var out []aurora.SessionSummary
	for id, session := range f.sessions {
		summary := session.SessionSummary
		summary.ID = id
		out = append(out, summary)
	}
	return out
}

func (f *fakeResumeRuntime) GetSession(id string) (aurora.SessionSnapshot, error) {
	return f.sessions[id], nil
}

func (f *fakeResumeRuntime) Retry(id string, _ aurora.RetryMode) (aurora.ProcessSnapshot, error) {
	f.retried = append(f.retried, id)
	return aurora.ProcessSnapshot{}, nil
}

func TestResumeInterruptedRetriesOnlyInterrupted(t *testing.T) {
	process := func(id string, status aurora.ProcessStatus) aurora.ProcessLog {
		return aurora.ProcessLog{ProcessSnapshot: aurora.ProcessSnapshot{ID: id, Status: status}}
	}
	rt := &fakeResumeRuntime{
		sessions: map[string]aurora.SessionSnapshot{
			// An interrupted process, plus a parked one that must not be touched.
			"ses_1": {SessionSummary: aurora.SessionSummary{ID: "ses_1"}, Processes: []aurora.ProcessLog{
				process("root", aurora.ProcessInterrupted),
				process("parked", aurora.ProcessWaitingTask),
			}},
			// A completed process never needs the kick.
			"ses_2": {SessionSummary: aurora.SessionSummary{ID: "ses_2"}, Processes: []aurora.ProcessLog{
				process("orphan", aurora.ProcessInterrupted),
				process("done", aurora.ProcessCompleted),
			}},
		},
	}
	d := &Dist{Runtime: rt, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	d.resumeInterrupted()

	sort.Strings(rt.retried)
	want := []string{"orphan", "root"}
	if len(rt.retried) != len(want) || rt.retried[0] != want[0] || rt.retried[1] != want[1] {
		t.Fatalf("retried = %v, want %v (parked/completed untouched)", rt.retried, want)
	}
}
