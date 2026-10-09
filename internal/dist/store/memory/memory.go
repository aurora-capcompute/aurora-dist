// Package memory provides in-memory implementations of the distribution's
// stores: an append-only event log and the sessions' scratch spaces. They suit
// tests and throwaway runs. Durable persistence lives in the sibling sqlite
// package.
package memory

import (
	"context"
	"maps"
	"slices"
	"sync"

	"github.com/aurora-capcompute/aurora-capcompute/aurora"
)

var _ aurora.EventLog = (*EventLog)(nil)

// EventLog is an in-memory append-only event log: one ordered stream per
// session.
type EventLog struct {
	mu      sync.RWMutex
	streams map[string][]aurora.LogEvent
}

func NewEventLog() *EventLog {
	return &EventLog{streams: make(map[string][]aurora.LogEvent)}
}

func (m *EventLog) Append(_ context.Context, sessionID string, events ...aurora.LogEvent) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, ev := range events {
		ev.Seq = uint64(len(m.streams[sessionID])) + 1
		m.streams[sessionID] = append(m.streams[sessionID], ev)
	}
	return uint64(len(m.streams[sessionID])), nil
}

func (m *EventLog) Read(_ context.Context, sessionID string, after uint64) ([]aurora.LogEvent, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	stream := m.streams[sessionID]
	return slices.Clone(stream[min(after, uint64(len(stream))):]), nil
}

// Sessions lists the sessions that have events: a stream exists only once an
// event is appended to it.
func (m *EventLog) Sessions(context.Context) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return slices.Sorted(maps.Keys(m.streams)), nil
}
