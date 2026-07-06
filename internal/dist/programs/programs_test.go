package programs

import (
	"context"
	"os"
	"path/filepath"
	"testing"
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
