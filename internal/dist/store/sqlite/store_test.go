package sqlite

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/aurora-capcompute/aurora-capcompute/aurora"
)

func TestEventLogAppendReadDurable(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "events.db")
	store, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	head, err := store.Append(ctx, "ses1",
		aurora.LogEvent{Kind: "process.state", Proc: "proc1", Rev: 1, Time: time.Unix(0, 0)},
		aurora.LogEvent{Kind: "process.state", Proc: "proc1", Rev: 1, Time: time.Unix(1, 0)},
	)
	if err != nil || head != 2 {
		t.Fatalf("append head=%d err=%v", head, err)
	}
	if _, err := store.Append(ctx, "ses2", aurora.LogEvent{Kind: "process.state"}); err != nil {
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

	events, err := reopened.Read(ctx, "ses1", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[0].Seq != 1 || events[1].Seq != 2 || events[1].Proc != "proc1" {
		t.Fatalf("read = %+v", events)
	}
	if tail, _ := reopened.Read(ctx, "ses1", 1); len(tail) != 1 || tail[0].Seq != 2 {
		t.Fatalf("read after 1 = %+v", tail)
	}
	sessions, err := reopened.Sessions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(sessions) != 2 || sessions[0] != "ses1" || sessions[1] != "ses2" {
		t.Fatalf("sessions = %+v", sessions)
	}
}

// Every commit reaches the disk before it returns: an intent the journal
// acknowledged must survive power loss, not just a process crash.
func TestStoreFsyncsEveryCommit(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "aurora.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()
	var mode int
	if err := store.db.QueryRow(`PRAGMA synchronous`).Scan(&mode); err != nil {
		t.Fatal(err)
	}
	if mode != 2 {
		t.Fatalf("synchronous = %d, want 2 (FULL)", mode)
	}
}
