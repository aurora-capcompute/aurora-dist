package sqlite

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/aurora-capcompute/aurora-capcompute/aurora"
	drivermem "github.com/aurora-capcompute/aurora-dispatchers/memory"
)

func TestEventLogAppendReadDurable(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "events.db")
	store, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	scope := aurora.LogScope{TenantID: "t", SessionID: "ses1"}

	head, err := store.Append(ctx, scope,
		aurora.LogEvent{Kind: "process.state", Proc: "proc1", Rev: 1, Time: time.Unix(0, 0)},
		aurora.LogEvent{Kind: "process.state", Proc: "proc1", Rev: 1, Time: time.Unix(1, 0)},
	)
	if err != nil || head != 2 {
		t.Fatalf("append head=%d err=%v", head, err)
	}
	if _, err := store.Append(ctx, aurora.LogScope{TenantID: "t", SessionID: "ses2"}, aurora.LogEvent{Kind: "process.state"}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen: the log must survive (durability) and read back in order.
	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	events, err := reopened.Read(ctx, scope, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[0].Seq != 1 || events[1].Seq != 2 || events[1].Proc != "proc1" {
		t.Fatalf("read = %+v", events)
	}
	if tail, _ := reopened.Read(ctx, scope, 1); len(tail) != 1 || tail[0].Seq != 2 {
		t.Fatalf("read after 1 = %+v", tail)
	}
	streams, err := reopened.Streams(ctx, "t")
	if err != nil {
		t.Fatal(err)
	}
	if len(streams) != 2 || streams[0].SessionID != "ses1" || streams[1].SessionID != "ses2" {
		t.Fatalf("streams = %+v", streams)
	}
}

// Compact must be one transaction — delete + insert applied atomically, the
// rewrite durable across a reopen, sibling streams untouched, appends
// continuing at the new head, and a failed compact leaving the old stream
// exactly as it was (never the delete without the insert).
func TestEventLogCompactAtomicDurable(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "compact.db")
	store, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	scope := aurora.LogScope{TenantID: "t", SessionID: "ses1"}
	other := aurora.LogScope{TenantID: "t", SessionID: "ses2"}
	for _, kind := range []string{"a", "b", "c", "d"} {
		if _, err := store.Append(ctx, scope, aurora.LogEvent{Kind: kind, Time: time.Unix(0, 0)}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.Append(ctx, other, aurora.LogEvent{Kind: "x", Time: time.Unix(0, 0)}); err != nil {
		t.Fatal(err)
	}

	// A compact that errors mid-flight applies nothing: the old stream survives
	// whole — the delete and the insert commit together or not at all.
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := store.Compact(cancelled, scope, []aurora.LogEvent{{Kind: "snapshot", Time: time.Unix(2, 0)}}); err == nil {
		t.Fatal("compact under a cancelled context must fail")
	}
	events, err := store.Read(ctx, scope, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 4 || events[0].Kind != "a" || events[3].Seq != 4 {
		t.Fatalf("failed compact mutated the stream: %+v", events)
	}

	if err := store.Compact(ctx, scope, []aurora.LogEvent{
		{Seq: 42, Kind: "snapshot", Proc: "", Time: time.Unix(2, 0), Data: json.RawMessage(`{"s":1}`)},
		{Kind: "d", Proc: "proc1", Rev: 3, Time: time.Unix(3, 0)},
	}); err != nil {
		t.Fatalf("compact: %v", err)
	}
	if head, err := store.Append(ctx, scope, aurora.LogEvent{Kind: "e", Time: time.Unix(4, 0)}); err != nil || head != 3 {
		t.Fatalf("append after compact head = %d, err = %v, want 3", head, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	// The rewrite survives a reopen: renumbered from 1, payloads and process
	// attribution intact, the sibling stream untouched.
	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	events, err = reopened.Read(ctx, scope, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 3 ||
		events[0].Seq != 1 || events[0].Kind != "snapshot" || string(events[0].Data) != `{"s":1}` ||
		events[1].Seq != 2 || events[1].Kind != "d" || events[1].Proc != "proc1" || events[1].Rev != 3 ||
		events[2].Seq != 3 || events[2].Kind != "e" {
		t.Fatalf("reopened compacted stream = %+v", events)
	}
	if sibling, _ := reopened.Read(ctx, other, 0); len(sibling) != 1 || sibling[0].Kind != "x" {
		t.Fatalf("sibling stream disturbed: %+v", sibling)
	}
	// Compacting to zero events erases the stream durably.
	if err := reopened.Compact(ctx, scope, nil); err != nil {
		t.Fatal(err)
	}
	if streams, _ := reopened.Streams(ctx, "t"); len(streams) != 1 || streams[0] != other {
		t.Fatalf("streams after erase = %+v, want only ses2", streams)
	}
}

func TestLeasesExclusivity(t *testing.T) {
	ctx := context.Background()
	store, err := Open(filepath.Join(t.TempDir(), "leases.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	now := time.Unix(1000, 0)

	ok, err := store.Acquire(ctx, "t", "process", "p1", "instanceA", now, time.Minute)
	if err != nil || !ok {
		t.Fatalf("first acquire ok=%v err=%v", ok, err)
	}
	// A different holder is rejected while the lease is live.
	if ok, _ := store.Acquire(ctx, "t", "process", "p1", "instanceB", now, time.Minute); ok {
		t.Fatal("second holder acquired a live lease")
	}
	// The same holder can renew.
	if ok, _ := store.Acquire(ctx, "t", "process", "p1", "instanceA", now, time.Minute); !ok {
		t.Fatal("holder could not renew")
	}
	// After expiry, another holder may take it.
	later := now.Add(2 * time.Minute)
	if ok, _ := store.Acquire(ctx, "t", "process", "p1", "instanceB", later, time.Minute); !ok {
		t.Fatal("could not acquire expired lease")
	}
	// Release by the owner frees it.
	if err := store.Release(ctx, "t", "process", "p1", "instanceB"); err != nil {
		t.Fatal(err)
	}
	if ok, _ := store.Acquire(ctx, "t", "process", "p1", "instanceA", later, time.Minute); !ok {
		t.Fatal("could not acquire after release")
	}
}

func TestMemoryKVDurable(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "kv.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}

	v1, err := store.Put(ctx, "t", "notes/a", json.RawMessage(`"one"`), []string{"untrusted_web"}, drivermem.PutAbsent, "")
	if err != nil || v1 != 1 {
		t.Fatalf("create = %d, %v", v1, err)
	}
	if _, err := store.Put(ctx, "t", "notes/a", json.RawMessage(`"x"`), nil, drivermem.PutAbsent, ""); !errors.Is(err, drivermem.ErrConflict) {
		t.Fatalf("create-over-existing = %v, want ErrConflict", err)
	}
	if _, err := store.Put(ctx, "t", "notes/a", json.RawMessage(`"x"`), nil, 99, ""); !errors.Is(err, drivermem.ErrConflict) {
		t.Fatalf("stale cas = %v, want ErrConflict", err)
	}
	if v2, err := store.Put(ctx, "t", "notes/a", json.RawMessage(`"two"`), []string{"secret"}, v1, ""); err != nil || v2 != 2 {
		t.Fatalf("cas = %d, %v", v2, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	// Values, labels, and versions survive a reopen.
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	value, labels, version, ok, err := reopened.Get(ctx, "t", "notes/a")
	if err != nil || !ok || string(value) != `"two"` || version != 2 || len(labels) != 1 || labels[0] != "secret" {
		t.Fatalf("get = %s labels=%v v=%d ok=%v err=%v", value, labels, version, ok, err)
	}
	if _, _, _, ok, _ := reopened.Get(ctx, "other", "notes/a"); ok {
		t.Fatal("tenants must not share values")
	}
	keys, err := reopened.List(ctx, "t", "notes/")
	if err != nil || len(keys) != 1 || keys[0] != "notes/a" {
		t.Fatalf("list = %v, %v", keys, err)
	}
}

// The durable activity memory is what makes the crash window exactly-once: a
// put re-driven after a restart replays the version its first execution
// recorded — in the same transaction as the write — instead of writing again.
func TestMemoryActivityExactlyOnceAcrossReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "activity.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}

	v1, err := store.Put(ctx, "t", "notes/a", json.RawMessage(`"one"`), nil, drivermem.PutAbsent, "act-1")
	if err != nil || v1 != 1 {
		t.Fatalf("create = %d, %v", v1, err)
	}
	// Same process, re-seen key: replay, not conflict, not a second bump.
	if again, err := store.Put(ctx, "t", "notes/a", json.RawMessage(`"ignored"`), nil, drivermem.PutAbsent, "act-1"); err != nil || again != 1 {
		t.Fatalf("re-driven create = %d, %v; want the recorded 1", again, err)
	}
	// CAS interplay: the recorded write replays; the version is not double-bumped.
	if v2, err := store.Put(ctx, "t", "notes/a", json.RawMessage(`"two"`), nil, 1, "act-2"); err != nil || v2 != 2 {
		t.Fatalf("cas = %d, %v", v2, err)
	}
	if again, err := store.Put(ctx, "t", "notes/a", json.RawMessage(`"two"`), nil, 1, "act-2"); err != nil || again != 2 {
		t.Fatalf("re-driven cas = %d, %v; want the recorded 2", again, err)
	}
	// A conflict is a non-effect and records nothing.
	if _, err := store.Put(ctx, "t", "notes/a", json.RawMessage(`"x"`), nil, drivermem.PutAbsent, "act-3"); !errors.Is(err, drivermem.ErrConflict) {
		t.Fatalf("conflicting put = %v, want ErrConflict", err)
	}
	if _, done, _ := store.Activity(ctx, "t", "act-3"); done {
		t.Fatal("a failed put must not record activity")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	// Crash-restart: the records survive with the values they guard.
	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if version, done, err := reopened.Activity(ctx, "t", "act-1"); err != nil || !done || version != 1 {
		t.Fatalf("activity after reopen = %d, %v, %v", version, done, err)
	}
	if again, err := reopened.Put(ctx, "t", "notes/a", json.RawMessage(`"ignored"`), nil, drivermem.PutAbsent, "act-1"); err != nil || again != 1 {
		t.Fatalf("re-driven create after reopen = %d, %v; want the recorded 1", again, err)
	}
	value, _, version, ok, err := reopened.Get(ctx, "t", "notes/a")
	if err != nil || !ok || string(value) != `"two"` || version != 2 {
		t.Fatalf("get = %s v=%d ok=%v err=%v; deduped puts must not re-write", value, version, ok, err)
	}
	// Records are tenant-scoped bookkeeping, invisible to Get/List.
	if _, done, _ := reopened.Activity(ctx, "other", "act-1"); done {
		t.Fatal("activity leaked across tenants")
	}
	if _, _, _, ok, _ := reopened.Get(ctx, "t", "act-1"); ok {
		t.Fatal("activity record surfaced through Get")
	}
	if keys, _ := reopened.List(ctx, "t", ""); len(keys) != 1 || keys[0] != "notes/a" {
		t.Fatalf("list = %v, want only notes/a", keys)
	}
	// "" bypasses the activity memory: keyless writes stay at-least-once.
	if v, err := reopened.Put(ctx, "t", "notes/a", json.RawMessage(`"lww"`), nil, drivermem.PutAny, ""); err != nil || v != 3 {
		t.Fatalf("keyless put = %d, %v", v, err)
	}
	if v, err := reopened.Put(ctx, "t", "notes/a", json.RawMessage(`"lww"`), nil, drivermem.PutAny, ""); err != nil || v != 4 {
		t.Fatalf("second keyless put = %d, %v; \"\" must never dedupe", v, err)
	}
	if _, done, _ := reopened.Activity(ctx, "t", ""); done {
		t.Fatal("empty activity key was recorded")
	}
}
