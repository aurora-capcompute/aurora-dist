package dist

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/aurora-capcompute/aurora-capcompute/aurora"
)

// The memory view: the tenant's durable memory exposed read-only to the
// operator plane, so the store agents write through core.memory can be browsed
// like a directory tree (its keys are slash-paths under the scope prefixes
// p/<processID>, s/<sessionID>, shared/<space>).
//
// Read-only is a security decision, not an omission: an operator write from
// here would bypass the journaled syscall path and its taint stamping, so a
// poisoned value could be laundered back in label-free. Reads are safe — and
// each carries the stored value's provenance labels, so a tainted value is
// visibly tainted when inspected.

// MemoryEntry is one stored key with the provenance labels of its value.
type MemoryEntry struct {
	Key    string   `json:"key"`
	Labels []string `json:"labels,omitempty"`
}

// MemoryValue is one stored value with its version and provenance labels.
// Found false means the key does not exist (not an error).
type MemoryValue struct {
	Key     string          `json:"key"`
	Found   bool            `json:"found"`
	Value   json.RawMessage `json:"value,omitempty"`
	Version int64           `json:"version,omitempty"`
	Labels  []string        `json:"labels,omitempty"`
}

// MemoryList lists the tenant's stored keys under a physical prefix ("" = all),
// each with its value's provenance labels.
func (d *Dist) MemoryList(ctx context.Context, prefix string) ([]MemoryEntry, error) {
	entries, err := d.memoryKV.List(ctx, d.tenant, strings.TrimSuffix(prefix, "/"))
	if err != nil {
		return nil, err
	}
	out := make([]MemoryEntry, 0, len(entries))
	for _, entry := range entries {
		out = append(out, MemoryEntry{Key: entry.Key, Labels: entry.Labels})
	}
	return out, nil
}

// MemoryValue reads one stored key by its physical path.
func (d *Dist) MemoryValue(ctx context.Context, key string) (MemoryValue, error) {
	if strings.TrimSpace(key) == "" {
		return MemoryValue{}, fmt.Errorf("%w: key is required", aurora.ErrInvalid)
	}
	value, labels, version, found, err := d.memoryKV.Get(ctx, d.tenant, key)
	if err != nil {
		return MemoryValue{}, err
	}
	if !found {
		return MemoryValue{Key: key}, nil
	}
	return MemoryValue{Key: key, Found: true, Value: value, Version: version, Labels: labels}, nil
}
