package sqlite

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"slices"
	"testing"

	drivermem "github.com/aurora-capcompute/aurora-dispatchers/memory"
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
	if _, err := store.ScratchSpace("t", "ses1").Put(ctx, "notes/a", json.RawMessage(`"kept"`), []string{"untrusted_web"}, drivermem.PutAbsent, ""); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened := openScratch(t, path)
	defer reopened.Close()
	value, labels, version, ok, err := reopened.ScratchSpace("t", "ses1").Get(ctx, "notes/a")
	if err != nil || !ok || string(value) != `"kept"` || version != 1 || !slices.Equal(labels, []string{"untrusted_web"}) {
		t.Fatalf("after reopen: value=%s labels=%v version=%d ok=%v err=%v", value, labels, version, ok, err)
	}
}

// One session cannot see another's keys, in either tenant or session.
func TestScratchSessionsAreIsolated(t *testing.T) {
	ctx := context.Background()
	store := openScratch(t, filepath.Join(t.TempDir(), "aurora.db"))
	defer store.Close()

	if _, err := store.ScratchSpace("t", "ses1").Put(ctx, "secret", json.RawMessage(`1`), nil, drivermem.PutAny, "act-1"); err != nil {
		t.Fatalf("put: %v", err)
	}
	for _, other := range [][2]string{{"t", "ses2"}, {"rival", "ses1"}} {
		space := store.ScratchSpace(other[0], other[1])
		if _, _, _, ok, _ := space.Get(ctx, "secret"); ok {
			t.Fatalf("%v read ses1's key", other)
		}
		if keys, _ := space.List(ctx, ""); len(keys) != 0 {
			t.Fatalf("%v listed %v", other, keys)
		}
		if _, done, _ := space.Activity(ctx, "act-1"); done {
			t.Fatalf("%v saw ses1's activity record", other)
		}
	}
}

// The store contract the driver relies on: compare-and-set, exactly-once puts
// under an activity key, conflicts that record nothing, and segment-wise list.
func TestScratchStoreContract(t *testing.T) {
	ctx := context.Background()
	store := openScratch(t, filepath.Join(t.TempDir(), "aurora.db"))
	defer store.Close()
	space := store.ScratchSpace("t", "ses1")

	if v, err := space.Put(ctx, "notes/a", json.RawMessage(`"one"`), nil, drivermem.PutAbsent, "act-1"); err != nil || v != 1 {
		t.Fatalf("create = %d, %v", v, err)
	}
	// A re-driven put replays its recorded version instead of conflicting.
	if v, err := space.Put(ctx, "notes/a", json.RawMessage(`"ignored"`), nil, drivermem.PutAbsent, "act-1"); err != nil || v != 1 {
		t.Fatalf("re-driven create = %d, %v; want the recorded 1", v, err)
	}
	if value, _, _, _, _ := space.Get(ctx, "notes/a"); string(value) != `"one"` {
		t.Fatalf("value = %s; a deduped put must not re-write", value)
	}
	// A conflict records nothing, so the same activity may succeed later.
	if _, err := space.Put(ctx, "notes/a", json.RawMessage(`"x"`), nil, 7, "act-2"); !errors.Is(err, drivermem.ErrConflict) {
		t.Fatalf("stale CAS = %v, want ErrConflict", err)
	}
	if v, err := space.Put(ctx, "notes/a", json.RawMessage(`"two"`), nil, 1, "act-2"); err != nil || v != 2 {
		t.Fatalf("CAS under act-2 = %d, %v", v, err)
	}

	for _, key := range []string{"notes2", "notes/b/c"} {
		if _, err := space.Put(ctx, key, json.RawMessage(`0`), nil, drivermem.PutAny, ""); err != nil {
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
