package memory

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"sync"

	drivermem "github.com/aurora-capcompute/aurora-dispatchers/memory"
)

var _ drivermem.Store = (*KV)(nil)

// KV is an in-memory implementation of the core.memory driver's Store: a
// versioned, provenance-preserving tenant KV. Values carry the labels they
// were written under so a later read re-surfaces them as tainted.
type KV struct {
	mu     sync.Mutex
	values map[string]kvEntry // tenant + "\x00" + key
}

type kvEntry struct {
	value   json.RawMessage
	labels  []string
	version int64
}

func NewKV() *KV { return &KV{values: make(map[string]kvEntry)} }

func kvKey(tenant, key string) string { return tenant + "\x00" + key }

func (s *KV) Get(_ context.Context, tenant, key string) (json.RawMessage, []string, int64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.values[kvKey(tenant, key)]
	if !ok {
		return nil, nil, 0, false, nil
	}
	return append(json.RawMessage(nil), entry.value...), append([]string(nil), entry.labels...), entry.version, true, nil
}

func (s *KV) Put(_ context.Context, tenant, key string, value json.RawMessage, labels []string, expect int64) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := kvKey(tenant, key)
	current, exists := s.values[id]
	switch {
	case expect == drivermem.PutAny:
	case expect == drivermem.PutAbsent && exists:
		return 0, drivermem.ErrConflict
	case expect > 0 && (!exists || current.version != expect):
		return 0, drivermem.ErrConflict
	}
	next := current.version + 1
	s.values[id] = kvEntry{
		value:   append(json.RawMessage(nil), value...),
		labels:  append([]string(nil), labels...),
		version: next,
	}
	return next, nil
}

func (s *KV) List(_ context.Context, tenant, prefix string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	marker := kvKey(tenant, prefix)
	var keys []string
	for id := range s.values {
		if strings.HasPrefix(id, marker) {
			keys = append(keys, strings.TrimPrefix(id, tenant+"\x00"))
		}
	}
	sort.Strings(keys)
	return keys, nil
}
