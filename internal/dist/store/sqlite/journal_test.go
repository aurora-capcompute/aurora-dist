package sqlite

import (
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/aurora-capcompute/capcompute/sys"
	"github.com/aurora-capcompute/capcompute/sys/replay/tape/journaled"
)

// The stored journal is driven by the kernel's real tape: records land
// hash-chained, survive reopening the database, replay identically, and pass
// the journaled.Verify audit.
func TestJournalBacksTheKernelTape(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal.db")
	store, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	header := journaled.Header{ABI: sys.ABIVersion, Program: "digest", Process: "proc_1"}
	// Args and results deliberately carry <, >, and & — they must round-trip
	// byte-identically or replay refuses its own history as a divergence.
	call := sys.Syscall{Abi: sys.ABIVersion, Name: "internet.fetch", Args: json.RawMessage(`{"url":"https://example.com?a=1&b=<2>"}`)}

	tape, err := journaled.NewTape(store.Journal("proc_1"), header)
	if err != nil {
		t.Fatalf("new tape: %v", err)
	}
	if _, err := tape.Begin(call); err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := tape.Commit(sys.Result(json.RawMessage(`{"status":200,"body":"<ok> & done"}`)).WithLabels("untrusted_web")); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if err := store.VerifyJournal("proc_1"); err != nil {
		t.Fatalf("verify: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopen: the journal replays, the chain still verifies, and a different
	// writer identity is refused.
	reopened, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	if err := reopened.VerifyJournal("proc_1"); err != nil {
		t.Fatalf("verify after reopen: %v", err)
	}
	replay, err := journaled.NewTape(reopened.Journal("proc_1"), header)
	if err != nil {
		t.Fatalf("replay tape: %v", err)
	}
	result, replayed, err := replay.Next(call)
	if err != nil || !replayed {
		t.Fatalf("next: replayed=%v err=%v", replayed, err)
	}
	if string(result.Result()) != `{"status":200,"body":"<ok> & done"}` {
		t.Fatalf("replayed result = %s", result.Result())
	}
	if labels := result.Labels(); len(labels) != 1 || labels[0] != "untrusted_web" {
		t.Fatalf("replayed labels = %v", labels)
	}
	if _, err := journaled.NewTape(reopened.Journal("proc_1"),
		journaled.Header{ABI: sys.ABIVersion, Program: "other", Process: "proc_1"}); err == nil {
		t.Fatal("expected ReplayIncompatibleError for a different program")
	}

	// Journals are isolated by id.
	if length := reopened.Journal("proc_2").Length(); length != 0 {
		t.Fatalf("unrelated journal length = %d", length)
	}
}

func TestJournalAppendEnforcesPosition(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "journal.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()
	journal := store.Journal("proc_1")
	if err := journal.SetHeader(journaled.Header{ABI: sys.ABIVersion, Program: "p", Process: "proc_1"}); err != nil {
		t.Fatalf("set header: %v", err)
	}
	call := sys.Syscall{Abi: sys.ABIVersion, Name: "x"}
	if err := journal.Append(journaled.Record{Position: 3, Kind: journaled.KindIntent, Syscall: &call}); err == nil {
		t.Fatal("expected position mismatch error")
	}
	if err := journal.Append(journaled.Record{Position: 0, Kind: journaled.KindIntent, Syscall: &call}); err != nil {
		t.Fatalf("append at 0: %v", err)
	}
	if journal.Length() != 1 {
		t.Fatalf("length = %d, want 1", journal.Length())
	}
}
