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
// were written under so a later read re-surfaces them as tainted. The
// activity memory — the executed-put records that make the driver's
// intent→completion crash window exactly-once — is a separate map, so Get and
// List cannot surface it, and shares the store's mutex, so the write and its
// record are atomic. Like the values it guards it is process-local: it covers
// re-drives within one host lifetime and just grows, which the in-memory
// posture (nothing survives a restart anyway) affords; the durable bound
// lives in the sqlite store.
type KV struct {
	mu         sync.Mutex
	values     map[string]kvEntry // tenant + "\x00" + key
	activities map[string]int64   // tenant + "\x00" + activity key → recorded version
}

type kvEntry struct {
	value   json.RawMessage
	labels  []string
	version int64
}

func NewKV() *KV {
	return &KV{values: make(map[string]kvEntry), activities: make(map[string]int64)}
}

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

func (s *KV) Put(_ context.Context, tenant, key string, value json.RawMessage, labels []string, expect int64, activity string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if activity != "" {
		if version, done := s.activities[kvKey(tenant, activity)]; done {
			return version, nil // this intent already wrote; replay its outcome
		}
	}
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
	if activity != "" {
		s.activities[kvKey(tenant, activity)] = next // same mutex hold as the write: atomic
	}
	return next, nil
}

func (s *KV) Activity(_ context.Context, tenant, activity string) (int64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	version, done := s.activities[kvKey(tenant, activity)]
	return version, done, nil
}

func (s *KV) List(_ context.Context, tenant, prefix string) ([]drivermem.ListedKey, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tenantPrefix := tenant + "\x00"
	var keys []drivermem.ListedKey
	for id, entry := range s.values {
		key, ok := strings.CutPrefix(id, tenantPrefix)
		if !ok {
			continue
		}
		// Segment-aware, tolerant of a trailing slash: "notes" and "notes/" both
		// list the "notes" subtree ("notes/a", …) but never the sibling "notes2".
		base := strings.TrimSuffix(prefix, "/")
		if prefix == "" || key == prefix || strings.HasPrefix(key, base+"/") {
			keys = append(keys, drivermem.ListedKey{Key: key, Labels: append([]string(nil), entry.labels...)})
		}
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i].Key < keys[j].Key })
	return keys, nil
}
