package programs

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/aurora-capcompute/aurora-capcompute/aurora"
)

func writeWasm(t *testing.T, dir, name string, content []byte) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), content, 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestDirListsWasmArtifacts(t *testing.T) {
	dir := t.TempDir()
	writeWasm(t, dir, "agent@1.wasm", []byte{0x00, 0x61, 0x73, 0x6d})
	writeWasm(t, dir, "beta.wasm", []byte{0x00, 0x61, 0x73, 0x6d, 0x01})
	writeWasm(t, dir, "notes.txt", []byte("ignored"))

	d := Dir{Path: dir}
	sources, err := d.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(sources) != 2 || sources[0].ID != "agent@1" || sources[1].ID != "beta" {
		t.Fatalf("sources = %+v", sources)
	}
	// No explicit default: the lexicographically first program.
	if id := d.DefaultID(); id != "agent@1" {
		t.Fatalf("default = %q", id)
	}
	if id := (Dir{Path: dir, Default: "beta"}).DefaultID(); id != "beta" {
		t.Fatalf("explicit default = %q", id)
	}
}

func TestDirToleratesMissingDirectory(t *testing.T) {
	d := Dir{Path: filepath.Join(t.TempDir(), "absent")}
	sources, err := d.List(context.Background())
	if err != nil || sources != nil {
		t.Fatalf("list = %v, %v", sources, err)
	}
	if id := d.DefaultID(); id != "" {
		t.Fatalf("default = %q", id)
	}
}

// retentionRuntime stubs the projection inputs of the retention query.
type retentionRuntime struct {
	aurora.Runtime
	artifacts []aurora.ProgramArtifact
	sessions  map[string]aurora.SessionSnapshot
}

func (r *retentionRuntime) Programs() []aurora.ProgramArtifact { return r.artifacts }

func (r *retentionRuntime) ListSessions() []aurora.SessionSummary {
	var out []aurora.SessionSummary
	for id := range r.sessions {
		out = append(out, aurora.SessionSummary{ID: id})
	}
	return out
}

func (r *retentionRuntime) GetSession(id string) (aurora.SessionSnapshot, error) {
	return r.sessions[id], nil
}

func TestRetentionGatesDecommissioning(t *testing.T) {
	runtime := &retentionRuntime{
		artifacts: []aurora.ProgramArtifact{{ID: "agent", Digest: "sha-new"}},
		sessions: map[string]aurora.SessionSnapshot{
			"ses_1": {Processes: []aurora.ProcessSnapshot{
				{ID: "proc_done", Status: aurora.ProcessCompleted, ProgramDigest: "sha-old"},
				{ID: "proc_parked", Status: aurora.ProcessWaitingTask, ProgramDigest: "sha-old"},
				{ID: "proc_live", Status: aurora.ProcessRunning, ProgramDigest: "sha-new"},
			}},
		},
	}
	refs := Retention(runtime)
	if len(refs) != 2 {
		t.Fatalf("refs = %+v", refs)
	}
	byDigest := map[string]Reference{}
	for _, ref := range refs {
		byDigest[ref.Digest] = ref
	}
	// The old digest is unregistered but a parked process still pins it: not
	// decommissionable. The completed process does not count.
	old := byDigest["sha-old"]
	if old.Decommissionable || len(old.Processes) != 1 || old.Processes[0] != "proc_parked" || len(old.Programs) != 0 {
		t.Fatalf("old = %+v", old)
	}
	current := byDigest["sha-new"]
	if current.Decommissionable || len(current.Programs) != 1 || current.Programs[0] != "agent" {
		t.Fatalf("current = %+v", current)
	}
}
