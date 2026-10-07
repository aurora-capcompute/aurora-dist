package memory

import (
	"sync"

	"github.com/aurora-capcompute/aurora-dispatchers/scratch"
)

// ScratchSpaces holds every session's core.scratch space in memory, one map
// store per session, created on first use.
type ScratchSpaces struct {
	mu     sync.Mutex
	spaces map[string]*scratch.MapStore
}

func NewScratchSpaces() *ScratchSpaces {
	return &ScratchSpaces{spaces: make(map[string]*scratch.MapStore)}
}

// ScratchSpace returns the session's space. Every call for one session returns
// the same store, so what one quantum writes the next one reads.
func (s *ScratchSpaces) ScratchSpace(session string) scratch.Store {
	s.mu.Lock()
	defer s.mu.Unlock()
	space, ok := s.spaces[session]
	if !ok {
		space = scratch.NewMapStore()
		s.spaces[session] = space
	}
	return space
}
