package memory

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/aurora-capcompute/aurora-capcompute/aurora"
	drivermem "github.com/aurora-capcompute/aurora-dispatchers/memory"
)

func TestProcessTableRoundTrip(t *testing.T) {
	table := NewProcessTable[string, aurora.ProcessContext]()
	ctx := context.Background()

	if _, err := table.LoadProcess(ctx, "missing"); !errors.Is(err, ErrProcessNotFound) {
		t.Fatalf("load missing = %v, want ErrProcessNotFound", err)
	}
	// A nil *Process round-trips like any other pointer; the table is a pure
	// lookup boundary and never dereferences what it stores.
	if err := table.SaveProcess(ctx, "proc_1@1", nil); err != nil {
		t.Fatalf("save: %v", err)
	}
	if process, err := table.LoadProcess(ctx, "proc_1@1"); err != nil || process != nil {
		t.Fatalf("load = %v, %v", process, err)
	}
}

func TestEventLogAppendReadStreams(t *testing.T) {
	log := NewEventLog()
	ctx := context.Background()
	scope := aurora.LogScope{TenantID: "t", SessionID: "ses"}

	head, err := log.Append(ctx, scope, aurora.LogEvent{Kind: "a"}, aurora.LogEvent{Kind: "b"})
	if err != nil || head != 2 {
		t.Fatalf("append head = %d, err = %v", head, err)
	}
	events, err := log.Read(ctx, scope, 1)
	if err != nil || len(events) != 1 || events[0].Kind != "b" || events[0].Seq != 2 {
		t.Fatalf("read = %+v, err = %v", events, err)
	}
	streams, err := log.Streams(ctx, "t")
	if err != nil || len(streams) != 1 || streams[0] != scope {
		t.Fatalf("streams = %v, err = %v", streams, err)
	}
}

func TestLeasesExcludeOtherHolders(t *testing.T) {
	leases := NewLeases()
	ctx := context.Background()
	now := time.Unix(0, 0)

	ok, err := leases.Acquire(ctx, "t", "process", "p1", "holder-a", now, time.Minute)
	if err != nil || !ok {
		t.Fatalf("first acquire = %v, %v", ok, err)
	}
	// A different holder is excluded until expiry; the same holder renews.
	if ok, _ := leases.Acquire(ctx, "t", "process", "p1", "holder-b", now.Add(time.Second), time.Minute); ok {
		t.Fatal("second holder acquired an unexpired lease")
	}
	if ok, _ := leases.Acquire(ctx, "t", "process", "p1", "holder-a", now.Add(time.Second), time.Minute); !ok {
		t.Fatal("holder could not renew its own lease")
	}
	if ok, _ := leases.Acquire(ctx, "t", "process", "p1", "holder-b", now.Add(2*time.Minute), time.Minute); !ok {
		t.Fatal("expired lease was not reacquirable")
	}
	if err := leases.Release(ctx, "t", "process", "p1", "holder-b"); err != nil {
		t.Fatalf("release: %v", err)
	}
	if ok, _ := leases.Acquire(ctx, "t", "process", "p1", "holder-a", now.Add(2*time.Minute), time.Minute); !ok {
		t.Fatal("released lease was not reacquirable")
	}
}

func TestKVListDoesNotLeakSiblingSubtree(t *testing.T) {
	kv := NewKV()
	ctx := context.Background()
	for _, key := range []string{"notes/a", "notes/b", "notes2/x", "notesX"} {
		if _, err := kv.Put(ctx, "t", key, json.RawMessage(`1`), nil, drivermem.PutAny, ""); err != nil {
			t.Fatal(err)
		}
	}
	// Both the bare and trailing-slash prefix list only the notes subtree — never
	// the sibling notes2 or the unrelated notesX.
	for _, prefix := range []string{"notes", "notes/"} {
		keys, err := kv.List(ctx, "t", prefix)
		if err != nil {
			t.Fatal(err)
		}
		if len(keys) != 2 || keys[0].Key != "notes/a" || keys[1].Key != "notes/b" {
			t.Fatalf("List(%q) = %v, want [notes/a notes/b]", prefix, keys)
		}
	}
}

func TestKVVersionedCompareAndSet(t *testing.T) {
	kv := NewKV()
	ctx := context.Background()

	if _, _, _, ok, err := kv.Get(ctx, "t", "notes/a"); ok || err != nil {
		t.Fatalf("get missing = %v, %v", ok, err)
	}
	v1, err := kv.Put(ctx, "t", "notes/a", json.RawMessage(`"one"`), []string{"untrusted_web"}, drivermem.PutAbsent, "")
	if err != nil || v1 != 1 {
		t.Fatalf("create = %d, %v", v1, err)
	}
	if _, err := kv.Put(ctx, "t", "notes/a", json.RawMessage(`"two"`), nil, drivermem.PutAbsent, ""); !errors.Is(err, drivermem.ErrConflict) {
		t.Fatalf("create-over-existing = %v, want ErrConflict", err)
	}
	if _, err := kv.Put(ctx, "t", "notes/a", json.RawMessage(`"two"`), nil, 99, ""); !errors.Is(err, drivermem.ErrConflict) {
		t.Fatalf("stale cas = %v, want ErrConflict", err)
	}
	v2, err := kv.Put(ctx, "t", "notes/a", json.RawMessage(`"two"`), nil, v1, "")
	if err != nil || v2 != 2 {
		t.Fatalf("cas = %d, %v", v2, err)
	}
	value, labels, version, ok, err := kv.Get(ctx, "t", "notes/a")
	if err != nil || !ok || string(value) != `"two"` || version != 2 || len(labels) != 0 {
		t.Fatalf("get = %s labels=%v v=%d ok=%v err=%v", value, labels, version, ok, err)
	}
	// Labels persist with the value they were written under.
	if _, labels, _, _, _ := kv.Get(ctx, "other", "notes/a"); labels != nil {
		t.Fatal("tenants must not share values")
	}
	if _, err := kv.Put(ctx, "t", "notes/b", json.RawMessage(`1`), nil, drivermem.PutAny, ""); err != nil {
		t.Fatal(err)
	}
	keys, err := kv.List(ctx, "t", "notes/")
	if err != nil || len(keys) != 2 || keys[0].Key != "notes/a" || keys[1].Key != "notes/b" {
		t.Fatalf("list = %v, %v", keys, err)
	}
}

// KV's activity memory: a re-driven put replays the recorded version instead
// of re-writing (a deduped CAS must not bump the version twice), conflicts
// record nothing, records are tenant-scoped and invisible to Get/List.
func TestKVActivityMemory(t *testing.T) {
	kv := NewKV()
	ctx := context.Background()

	v1, err := kv.Put(ctx, "t", "notes/a", json.RawMessage(`"one"`), nil, drivermem.PutAbsent, "act-1")
	if err != nil || v1 != 1 {
		t.Fatalf("create = %d, %v", v1, err)
	}
	// Re-executing create-only would conflict; the recorded key replays instead.
	if again, err := kv.Put(ctx, "t", "notes/a", json.RawMessage(`"ignored"`), nil, drivermem.PutAbsent, "act-1"); err != nil || again != 1 {
		t.Fatalf("re-driven create = %d, %v; want the recorded 1", again, err)
	}
	value, _, version, ok, err := kv.Get(ctx, "t", "notes/a")
	if err != nil || !ok || string(value) != `"one"` || version != 1 {
		t.Fatalf("get = %s v=%d ok=%v err=%v; deduped put must not re-write", value, version, ok, err)
	}
	if version, done, err := kv.Activity(ctx, "t", "act-1"); err != nil || !done || version != 1 {
		t.Fatalf("activity = %d, %v, %v", version, done, err)
	}
	if _, done, _ := kv.Activity(ctx, "other", "act-1"); done {
		t.Fatal("activity leaked across tenants")
	}
	// CAS interplay: the recorded write replays; the version is not double-bumped.
	if v2, err := kv.Put(ctx, "t", "notes/a", json.RawMessage(`"two"`), nil, 1, "act-2"); err != nil || v2 != 2 {
		t.Fatalf("cas = %d, %v", v2, err)
	}
	if again, err := kv.Put(ctx, "t", "notes/a", json.RawMessage(`"two"`), nil, 1, "act-2"); err != nil || again != 2 {
		t.Fatalf("re-driven cas = %d, %v; want the recorded 2", again, err)
	}
	if _, _, version, _, _ := kv.Get(ctx, "t", "notes/a"); version != 2 {
		t.Fatalf("version = %d, want 2 (not double-bumped)", version)
	}
	// A conflict is a non-effect and records nothing.
	if _, err := kv.Put(ctx, "t", "notes/a", json.RawMessage(`"x"`), nil, drivermem.PutAbsent, "act-3"); !errors.Is(err, drivermem.ErrConflict) {
		t.Fatalf("conflicting put = %v, want ErrConflict", err)
	}
	if _, done, _ := kv.Activity(ctx, "t", "act-3"); done {
		t.Fatal("a failed put must not record activity")
	}
	// Activity records never surface as data: not through Get, not through List.
	if _, _, _, ok, _ := kv.Get(ctx, "t", "act-1"); ok {
		t.Fatal("activity record surfaced through Get")
	}
	if keys, _ := kv.List(ctx, "t", ""); len(keys) != 1 || keys[0].Key != "notes/a" {
		t.Fatalf("list = %v, want only notes/a", keys)
	}
	// "" bypasses the activity memory: keyless writes stay at-least-once.
	if v, err := kv.Put(ctx, "t", "notes/a", json.RawMessage(`"lww"`), nil, drivermem.PutAny, ""); err != nil || v != 3 {
		t.Fatalf("keyless put = %d, %v", v, err)
	}
	if v, err := kv.Put(ctx, "t", "notes/a", json.RawMessage(`"lww"`), nil, drivermem.PutAny, ""); err != nil || v != 4 {
		t.Fatalf("second keyless put = %d, %v; \"\" must never dedupe", v, err)
	}
	if _, done, _ := kv.Activity(ctx, "t", ""); done {
		t.Fatal("empty activity key was recorded")
	}
}
