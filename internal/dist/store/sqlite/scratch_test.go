package sqlite

import (
	"context"
	"encoding/json"
	"path/filepath"
	"slices"
	"testing"
)

func openScratch(t *testing.T, path string) *Store {
	t.Helper()
	store, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return store
}

// A session's scratch is durable: a value and its labels survive a
// reopen of the database — a host restart between two processes of the session.
func TestScratchSurvivesReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "aurora.db")

	store := openScratch(t, path)
	if err := store.ScratchSpace("ses1").Put(ctx, "notes/a", json.RawMessage(`"kept"`), []string{"untrusted_web"}); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened := openScratch(t, path)
	defer reopened.Close()
	value, labels, ok, err := reopened.ScratchSpace("ses1").Get(ctx, "notes/a")
	if err != nil || !ok || string(value) != `"kept"` || !slices.Equal(labels, []string{"untrusted_web"}) {
		t.Fatalf("after reopen: value=%s labels=%v ok=%v err=%v", value, labels, ok, err)
	}
}

// One session cannot see another's keys.
func TestScratchSessionsAreIsolated(t *testing.T) {
	ctx := context.Background()
	store := openScratch(t, filepath.Join(t.TempDir(), "aurora.db"))
	defer store.Close()

	if err := store.ScratchSpace("ses1").Put(ctx, "secret", json.RawMessage(`1`), nil); err != nil {
		t.Fatalf("put: %v", err)
	}
	other := store.ScratchSpace("ses2")
	if _, _, ok, _ := other.Get(ctx, "secret"); ok {
		t.Fatal("ses2 read ses1's key")
	}
	if keys, _ := other.List(ctx, ""); len(keys) != 0 {
		t.Fatalf("ses2 listed %v", keys)
	}
}

// The store contract the driver relies on: a put replaces the value, and list
// is segment-wise.
func TestScratchStoreContract(t *testing.T) {
	ctx := context.Background()
	store := openScratch(t, filepath.Join(t.TempDir(), "aurora.db"))
	defer store.Close()
	space := store.ScratchSpace("ses1")

	for _, value := range []string{`"one"`, `"two"`} {
		if err := space.Put(ctx, "notes/a", json.RawMessage(value), nil); err != nil {
			t.Fatal(err)
		}
	}
	if value, _, _, _ := space.Get(ctx, "notes/a"); string(value) != `"two"` {
		t.Fatalf("value = %s, want the second put", value)
	}

	for _, key := range []string{"notes2", "notes/b/c"} {
		if err := space.Put(ctx, key, json.RawMessage(`0`), nil); err != nil {
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
