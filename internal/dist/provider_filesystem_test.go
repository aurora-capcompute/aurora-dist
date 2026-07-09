package dist

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/aurora-capcompute/aurora-capcompute/aurora"
	"github.com/aurora-capcompute/aurora-dispatchers/registry"
	"github.com/aurora-capcompute/capcompute/sys"
)

// The distribution wires core.filesystem into its provider, so a manifest that
// grants it produces a dispatcher that publishes the capability and serves a
// read from within the grant's root — read-only, root-confined, whole file or a
// line range.
func TestProviderServesFilesystemRead(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "note.txt"), []byte("line one\nline two\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	provider := newProvider(
		[]registry.Registration{registry.FilesystemRegistration{}},
		registry.Services{},
	)
	manifest := aurora.Manifest{
		Version: aurora.ManifestVersion,
		Syscalls: []aurora.Syscall{
			{Syscall: "core.filesystem", Config: json.RawMessage(`{"capabilities":[{"operation":"read"}],"roots":["` + dir + `"]}`)},
		},
	}
	dispatcher, err := provider.NewDispatcher(context.Background(), aurora.ProcessContext{}, manifest)
	if err != nil {
		t.Fatalf("new dispatcher: %v", err)
	}

	published := false
	for _, capability := range dispatcher.Capabilities() {
		if capability.Name == "core.filesystem" {
			published = true
		}
	}
	if !published {
		t.Fatalf("core.filesystem was not published: %v", dispatcher.Capabilities())
	}

	result, err := dispatcher.Dispatch(context.Background(), aurora.ProcessContext{}, sys.Syscall{
		Name: "core.filesystem",
		Args: json.RawMessage(`{"operation":"read","path":"note.txt","start_line":2,"end_line":2}`),
	}, sys.Authorization{})
	if err != nil {
		t.Fatalf("dispatch read: %v", err)
	}
	if result.Status() != sys.StatusResult {
		t.Fatalf("read status = %v (%s): %s", result.Status(), result.Errno(), result.Message())
	}
	var response struct {
		Content    string `json:"content"`
		TotalLines int    `json:"total_lines"`
	}
	if err := json.Unmarshal(result.Result(), &response); err != nil {
		t.Fatalf("decode read response: %v", err)
	}
	if response.Content != "line two\n" {
		t.Fatalf("content = %q, want the second line", response.Content)
	}
	if response.TotalLines != 2 {
		t.Fatalf("total_lines = %d, want 2", response.TotalLines)
	}
}

// A read outside the grant's root is refused: the root is the security boundary,
// not a hint.
func TestProviderFilesystemRootConfinement(t *testing.T) {
	dir := t.TempDir()
	provider := newProvider(
		[]registry.Registration{registry.FilesystemRegistration{}},
		registry.Services{},
	)
	manifest := aurora.Manifest{
		Version: aurora.ManifestVersion,
		Syscalls: []aurora.Syscall{
			{Syscall: "core.filesystem", Config: json.RawMessage(`{"capabilities":[{"operation":"read"}],"roots":["` + dir + `"]}`)},
		},
	}
	dispatcher, err := provider.NewDispatcher(context.Background(), aurora.ProcessContext{}, manifest)
	if err != nil {
		t.Fatalf("new dispatcher: %v", err)
	}
	result, err := dispatcher.Dispatch(context.Background(), aurora.ProcessContext{}, sys.Syscall{
		Name: "core.filesystem",
		Args: json.RawMessage(`{"operation":"read","path":"../../../../etc/hostname"}`),
	}, sys.Authorization{})
	if err != nil {
		t.Fatalf("dispatch read: %v", err)
	}
	if result.Status() != sys.StatusFailed {
		t.Fatalf("a read escaping the root must fail, got %v: %s", result.Status(), result.Result())
	}
}
