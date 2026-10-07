package memory

import (
	"sync"

	drivermem "github.com/aurora-capcompute/aurora-dispatchers/memory"
)

// ScratchSpaces holds every session's core.scratch space in memory, one map
// store per (tenant, session), created on first use.
type ScratchSpaces struct {
	mu     sync.Mutex
	spaces map[scratchKey]*drivermem.MapStore
}

type scratchKey struct{ tenant, session string }

func NewScratchSpaces() *ScratchSpaces {
	return &ScratchSpaces{spaces: make(map[scratchKey]*drivermem.MapStore)}
}

// ScratchSpace returns the session's space. Every call for one session returns
// the same store, so what one quantum writes the next one reads.
func (s *ScratchSpaces) ScratchSpace(tenant, session string) drivermem.Store {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := scratchKey{tenant: tenant, session: session}
	space, ok := s.spaces[key]
	if !ok {
		space = drivermem.NewMapStore()
		s.spaces[key] = space
	}
	return space
}
