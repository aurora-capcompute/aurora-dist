package sqlite

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"slices"
	"testing"

	"github.com/aurora-capcompute/aurora-dispatchers/scratch"
)

func openScratch(t *testing.T, path string) *Store {
	t.Helper()
	store, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return store
}

// A session's scratch is durable: a value, its labels and its version survive a
// reopen of the database — a host restart between two processes of the session.
func TestScratchSurvivesReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "aurora.db")

	store := openScratch(t, path)
	if _, err := store.ScratchSpace("ses1").Put(ctx, "notes/a", json.RawMessage(`"kept"`), []string{"untrusted_web"}, scratch.PutAbsent); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened := openScratch(t, path)
	defer reopened.Close()
	value, labels, version, ok, err := reopened.ScratchSpace("ses1").Get(ctx, "notes/a")
	if err != nil || !ok || string(value) != `"kept"` || version != 1 || !slices.Equal(labels, []string{"untrusted_web"}) {
		t.Fatalf("after reopen: value=%s labels=%v version=%d ok=%v err=%v", value, labels, version, ok, err)
	}
}

// One session cannot see another's keys.
func TestScratchSessionsAreIsolated(t *testing.T) {
	ctx := context.Background()
	store := openScratch(t, filepath.Join(t.TempDir(), "aurora.db"))
	defer store.Close()

	if _, err := store.ScratchSpace("ses1").Put(ctx, "secret", json.RawMessage(`1`), nil, scratch.PutAny); err != nil {
		t.Fatalf("put: %v", err)
	}
	other := store.ScratchSpace("ses2")
	if _, _, _, ok, _ := other.Get(ctx, "secret"); ok {
		t.Fatal("ses2 read ses1's key")
	}
	if keys, _ := other.List(ctx, ""); len(keys) != 0 {
		t.Fatalf("ses2 listed %v", keys)
	}
}

// The store contract the driver relies on: compare-and-set, refusals that change
// nothing, and segment-wise list.
func TestScratchStoreContract(t *testing.T) {
	ctx := context.Background()
	store := openScratch(t, filepath.Join(t.TempDir(), "aurora.db"))
	defer store.Close()
	space := store.ScratchSpace("ses1")

	if v, err := space.Put(ctx, "notes/a", json.RawMessage(`"one"`), nil, scratch.PutAbsent); err != nil || v != 1 {
		t.Fatalf("create = %d, %v", v, err)
	}
	if _, err := space.Put(ctx, "notes/a", json.RawMessage(`"again"`), nil, scratch.PutAbsent); !errors.Is(err, scratch.ErrConflict) {
		t.Fatalf("create on an existing key = %v, want ErrConflict", err)
	}
	if _, err := space.Put(ctx, "notes/a", json.RawMessage(`"x"`), nil, 7); !errors.Is(err, scratch.ErrConflict) {
		t.Fatalf("stale CAS = %v, want ErrConflict", err)
	}
	if value, _, _, _, _ := space.Get(ctx, "notes/a"); string(value) != `"one"` {
		t.Fatalf("value = %s; a refused put must change nothing", value)
	}
	if v, err := space.Put(ctx, "notes/a", json.RawMessage(`"two"`), nil, 1); err != nil || v != 2 {
		t.Fatalf("CAS = %d, %v", v, err)
	}

	for _, key := range []string{"notes2", "notes/b/c"} {
		if _, err := space.Put(ctx, key, json.RawMessage(`0`), nil, scratch.PutAny); err != nil {
			t.Fatal(err)
		}
	}
	keys, err := space.List(ctx, "notes")
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, key := range keys {
		names = append(names, key.Key)
	}
	if !slices.Equal(names, []string{"notes/a", "notes/b/c"}) {
		t.Fatalf("list notes = %v, want the notes subtree only", names)
	}
}
