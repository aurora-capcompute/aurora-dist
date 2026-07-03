package memory

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/aurora-capcompute/capcompute"
)

// ErrProcessNotFound reports a PID with no saved process instance. The
// runtime reacts by reactivating the process from its journal — the journal,
// not the instance, is the durable process, so a process table is
// legitimately in-memory even in durable assemblies.
var ErrProcessNotFound = errors.New("process not found")

// ProcessTable is an in-memory capcompute.ProcessTable: the kernel's per-PID
// process lookup boundary. Instantiate it with the runtime's credential type,
// e.g. memory.NewProcessTable[string, aurora.ProcessContext]().
type ProcessTable[ID comparable, K capcompute.PID[ID]] struct {
	mu        sync.Mutex
	processes map[ID]*capcompute.Process[K]
}

func NewProcessTable[ID comparable, K capcompute.PID[ID]]() *ProcessTable[ID, K] {
	return &ProcessTable[ID, K]{
		processes: make(map[ID]*capcompute.Process[K]),
	}
}

func (t *ProcessTable[ID, K]) LoadProcess(_ context.Context, pid ID) (*capcompute.Process[K], error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	process, ok := t.processes[pid]
	if !ok {
		return nil, fmt.Errorf("%w: %v", ErrProcessNotFound, pid)
	}
	return process, nil
}

func (t *ProcessTable[ID, K]) SaveProcess(_ context.Context, pid ID, process *capcompute.Process[K]) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.processes == nil {
		t.processes = make(map[ID]*capcompute.Process[K])
	}
	t.processes[pid] = process
	return nil
}
