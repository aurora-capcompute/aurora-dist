// Package memory provides in-memory implementations of the distribution's
// stores: an append-only event log and the sessions' scratch spaces. They suit tests and throwaway runs; durable persistence lives in the
// sibling sqlite package.
package memory

import (
	"context"
	"sort"
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
	existing := m.streams[sessionID]
	head := uint64(len(existing))
	if len(events) == 0 {
		return head, nil
	}
	appended := make([]aurora.LogEvent, len(events))
	for i, ev := range events {
		head++
		ev.Seq = head
		ev.Data = append([]byte(nil), ev.Data...)
		appended[i] = ev
	}
	m.streams[sessionID] = append(existing, appended...)
	return head, nil
}

func (m *EventLog) Read(_ context.Context, sessionID string, after uint64) ([]aurora.LogEvent, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	stream := m.streams[sessionID]
	if after >= uint64(len(stream)) {
		return nil, nil
	}
	out := make([]aurora.LogEvent, 0, uint64(len(stream))-after)
	for _, ev := range stream[after:] {
		ev.Data = append([]byte(nil), ev.Data...)
		out = append(out, ev)
	}
	return out, nil
}

func (m *EventLog) Sessions(context.Context) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []string
	for sessionID, stream := range m.streams {
		if len(stream) > 0 {
			out = append(out, sessionID)
		}
	}
	sort.Strings(out)
	return out, nil
}
