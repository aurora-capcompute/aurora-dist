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
	graphs   map[string]aurora.SessionGraph
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

func (f *fakeResumeRuntime) SessionGraph(id string) (aurora.SessionGraph, error) {
	return f.graphs[id], nil
}

func (f *fakeResumeRuntime) Retry(id string, _ aurora.RetryMode) (aurora.ProcessSnapshot, error) {
	f.retried = append(f.retried, id)
	return aurora.ProcessSnapshot{}, nil
}

func TestResumeInterruptedRetriesTopmostOnly(t *testing.T) {
	rt := &fakeResumeRuntime{
		sessions: map[string]aurora.SessionSnapshot{
			// A root interrupted with an interrupted child mid-flight, plus a
			// parked sibling that must not be touched.
			"ses_1": {SessionSummary: aurora.SessionSummary{ID: "ses_1"}, Processes: []aurora.ProcessSnapshot{
				{ID: "root", Status: aurora.ProcessInterrupted},
				{ID: "child", Status: aurora.ProcessInterrupted},
				{ID: "parked", Status: aurora.ProcessWaitingTask},
			}},
			// An interrupted child whose parent merely yielded (waiting on it)
			// needs the kick; a completed process never does.
			"ses_2": {SessionSummary: aurora.SessionSummary{ID: "ses_2"}, Processes: []aurora.ProcessSnapshot{
				{ID: "yielded_parent", Status: aurora.ProcessYielded},
				{ID: "orphan", Status: aurora.ProcessInterrupted},
				{ID: "done", Status: aurora.ProcessCompleted},
			}},
		},
		graphs: map[string]aurora.SessionGraph{
			"ses_1": {Processes: []aurora.SessionGraphProcess{
				{ProcessID: "root"},
				{ProcessID: "child", ParentProcessID: "root"},
				{ProcessID: "parked"},
			}},
			"ses_2": {Processes: []aurora.SessionGraphProcess{
				{ProcessID: "yielded_parent"},
				{ProcessID: "orphan", ParentProcessID: "yielded_parent"},
				{ProcessID: "done"},
			}},
		},
	}
	d := &Dist{Runtime: rt, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	d.resumeInterrupted()

	sort.Strings(rt.retried)
	want := []string{"orphan", "root"}
	if len(rt.retried) != len(want) || rt.retried[0] != want[0] || rt.retried[1] != want[1] {
		t.Fatalf("retried = %v, want %v (root re-drives its interrupted child; parked/yielded/completed untouched)", rt.retried, want)
	}
}

func TestStableInstanceIDPersistsAcrossCalls(t *testing.T) {
	dir := t.TempDir()
	first, err := stableInstanceID(dir)
	if err != nil || first == "" {
		t.Fatalf("first = %q, err = %v", first, err)
	}
	second, err := stableInstanceID(dir)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("instance id not stable across restarts: %q vs %q", first, second)
	}
}
