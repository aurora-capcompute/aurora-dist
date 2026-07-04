package dist

import "github.com/aurora-capcompute/aurora-capcompute/aurora"

// SessionLog is the distribution's one comprehensive read: everything the
// event log holds for a session, folded once into typed domain objects. The
// session is the log stream (tenant → session → process → revision), so a
// single fetch carries the session metadata, its conversation history, and
// every process with its full state, delegation links, complete journal
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
// its delegation links, the flat journal of every entry ever written (each
// carrying its position and the revision that produced it, so the fork
// structure — and thus any single revision's effective journal — is
// reconstructible), and its tasks.
type ProcessLog struct {
	aurora.ProcessSnapshot
	ParentProcessID string                `json:"parent_process_id,omitempty"`
	ChildProcessIDs []string              `json:"child_process_ids,omitempty"`
	Entries         []aurora.JournalEntry `json:"entries"`
	Tasks           []aurora.TaskSnapshot `json:"tasks,omitempty"`
}

// SessionLog folds one session's whole state into a single projection. It
// composes the runtime's read primitives — the session snapshot (metadata,
// history, per-process fields), the session graph (entries across revisions,
// delegation links), and each process's tasks — into the shape a terminal
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
	type graphInfo struct {
		parent   string
		children []string
		entries  []aurora.JournalEntry
	}
	byProcess := make(map[string]graphInfo, len(graph.Processes))
	for _, gp := range graph.Processes {
		byProcess[gp.ProcessID] = graphInfo{
			parent:   gp.ParentProcessID,
			children: gp.ChildProcessIDs,
			entries:  gp.Entries,
		}
	}

	processes := make([]ProcessLog, 0, len(session.Processes))
	for _, snapshot := range session.Processes {
		info := byProcess[snapshot.ID]
		entries := info.entries
		if entries == nil {
			entries = []aurora.JournalEntry{}
		}
		tasks, err := d.Runtime.Tasks(snapshot.ID)
		if err != nil {
			return SessionLog{}, err
		}
		processes = append(processes, ProcessLog{
			ProcessSnapshot: snapshot,
			ParentProcessID: info.parent,
			ChildProcessIDs: info.children,
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
