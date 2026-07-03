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

	v1, err := store.Put(ctx, "t", "notes/a", json.RawMessage(`"one"`), []string{"untrusted_web"}, drivermem.PutAbsent)
	if err != nil || v1 != 1 {
		t.Fatalf("create = %d, %v", v1, err)
	}
	if _, err := store.Put(ctx, "t", "notes/a", json.RawMessage(`"x"`), nil, drivermem.PutAbsent); !errors.Is(err, drivermem.ErrConflict) {
		t.Fatalf("create-over-existing = %v, want ErrConflict", err)
	}
	if _, err := store.Put(ctx, "t", "notes/a", json.RawMessage(`"x"`), nil, 99); !errors.Is(err, drivermem.ErrConflict) {
		t.Fatalf("stale cas = %v, want ErrConflict", err)
	}
	if v2, err := store.Put(ctx, "t", "notes/a", json.RawMessage(`"two"`), []string{"secret"}, v1); err != nil || v2 != 2 {
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
