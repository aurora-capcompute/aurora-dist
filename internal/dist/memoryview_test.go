package dist

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/aurora-capcompute/aurora-capcompute/aurora"
	drivermem "github.com/aurora-capcompute/aurora-dispatchers/memory"
)

// The memory view is the driver's own store read back: what core.memory writes
// under the tenant is what MemoryList/MemoryValue surface — keys, values,
// versions, and the provenance labels that make a poisoned value visibly
// tainted to the inspecting operator.
func TestMemoryViewReadsTheTenantStore(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d, err := New(ctx, Config{
		ProgramsDir: t.TempDir(),
		TaskSecret:  []byte("view-secret"),
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("assemble distribution: %v", err)
	}
	defer func() { _ = d.Close(context.Background()) }()

	// Seed through the same store the core.memory driver writes (the view is in
	// this package precisely so the test can reach it without a write API).
	seed := func(key, value string, labels ...string) {
		t.Helper()
		if _, err := d.memoryKV.Put(ctx, d.tenant, key, json.RawMessage(value), labels, drivermem.PutAny, ""); err != nil {
			t.Fatalf("seed %s: %v", key, err)
		}
	}
	seed("shared/team-kb/notes/today", `"standup at 10"`, "untrusted_web")
	seed("shared/team-kb/handoff", `{"owner":"alice"}`)
	seed("s/ses_1/prefs/tone", `"formal"`)

	// List everything, and list under a prefix.
	all, err := d.MemoryList(ctx, "")
	if err != nil || len(all) != 3 {
		t.Fatalf("list all = %v, %v; want 3 keys", all, err)
	}
	kb, err := d.MemoryList(ctx, "shared/team-kb")
	if err != nil || len(kb) != 2 {
		t.Fatalf("list shared/team-kb = %v, %v; want 2 keys", kb, err)
	}
	// Labels ride the listing, so provenance is visible before any value is read.
	var labelled bool
	for _, entry := range kb {
		if entry.Key == "shared/team-kb/notes/today" {
			labelled = len(entry.Labels) == 1 && entry.Labels[0] == "untrusted_web"
		}
	}
	if !labelled {
		t.Fatalf("listing lost the value's provenance labels: %+v", kb)
	}

	// A value comes back with its version and labels.
	value, err := d.MemoryValue(ctx, "shared/team-kb/notes/today")
	if err != nil {
		t.Fatalf("value: %v", err)
	}
	if !value.Found || string(value.Value) != `"standup at 10"` || value.Version != 1 ||
		len(value.Labels) != 1 || value.Labels[0] != "untrusted_web" {
		t.Fatalf("value = %+v", value)
	}

	// A missing key is found:false, not an error; an empty key is invalid.
	if missing, err := d.MemoryValue(ctx, "shared/absent"); err != nil || missing.Found {
		t.Fatalf("missing = %+v, %v", missing, err)
	}
	if _, err := d.MemoryValue(ctx, ""); !errors.Is(err, aurora.ErrInvalid) {
		t.Fatalf("empty key err = %v, want ErrInvalid", err)
	}
}
