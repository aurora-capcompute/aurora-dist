package dist

import "github.com/aurora-capcompute/aurora-capcompute/aurora"

// SessionLog is the distribution's one comprehensive read: everything the
// event log holds for a session, folded once into typed domain objects. The
// session is the log stream (tenant → session → process → revision), so a
// single fetch carries the session metadata, its conversation history, and
// every process with its full state, complete journal
// across all revisions, and tasks. Every other view a terminal wants — the
// current journal, a specific revision, the call graph, a task list — is a
// grouping of this payload, computed on the client. The server owns the fold
// (mechanism); rendering is the terminal's (policy).
type SessionLog struct {
	Session   aurora.SessionSummary   `json:"session"`
	History   []aurora.HistoryMessage `json:"history,omitempty"`
	Processes []ProcessLog            `json:"processes"`
}

// ProcessLog is one process's complete durable state: its snapshot fields,
// the flat journal of every entry ever written (each
// carrying its position and the revision that produced it, so the fork
// structure — and thus any single revision's effective journal — is
// reconstructible), and its tasks.
type ProcessLog struct {
	aurora.ProcessSnapshot
	Entries []aurora.JournalEntry `json:"entries"`
	Tasks   []aurora.TaskSnapshot `json:"tasks,omitempty"`
}

// SessionLog folds one session's whole state into a single projection. It
// composes the runtime's read primitives — the session snapshot (metadata,
// history, per-process fields), the session graph (entries across revisions,
// and each process's tasks — into the shape a terminal
// renders from. The runtime keeps those primitives; the distribution is where
// they merge into the one read the API exposes.
func (d *Dist) SessionLog(sessionID string) (SessionLog, error) {
	session, err := d.Runtime.GetSession(sessionID)
	if err != nil {
		return SessionLog{}, err
	}
	graph, err := d.Runtime.SessionGraph(sessionID)
	if err != nil {
		return SessionLog{}, err
	}
	byProcess := make(map[string][]aurora.JournalEntry, len(graph.Processes))
	for _, gp := range graph.Processes {
		byProcess[gp.ProcessID] = gp.Entries
	}

	processes := make([]ProcessLog, 0, len(session.Processes))
	for _, snapshot := range session.Processes {
		tasks, err := d.Runtime.Tasks(snapshot.ID)
		if err != nil {
			return SessionLog{}, err
		}
		// The graph carries the journal across every revision; the snapshot
		// carries the process's current state.
		entries := byProcess[snapshot.ID]
		if entries == nil {
			entries = []aurora.JournalEntry{}
		}
		processes = append(processes, ProcessLog{
			ProcessSnapshot: snapshot,
			Entries:         entries,
			Tasks:           tasks,
		})
	}
	return SessionLog{
		Session:   session.SessionSummary,
		History:   session.History,
		Processes: processes,
	}, nil
}
