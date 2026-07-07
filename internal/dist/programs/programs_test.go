package programs

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func writeFile(t *testing.T, dir, name string, content []byte) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), content, 0o600); err != nil {
		t.Fatal(err)
	}
}

// writeProgram drops the pair a program directory loads: <id>.wasm and its
// <id>.json interface manifest.
func writeProgram(t *testing.T, dir, id string, wasm []byte) {
	t.Helper()
	writeFile(t, dir, id+".wasm", wasm)
	writeFile(t, dir, id+".json", []byte(
		`{"description":"a program","input":{"type":"string"},"output":{"type":"string"}}`))
}

func TestDirListsWasmArtifacts(t *testing.T) {
	dir := t.TempDir()
	writeProgram(t, dir, "agent@1", []byte{0x00, 0x61, 0x73, 0x6d})
	writeProgram(t, dir, "beta", []byte{0x00, 0x61, 0x73, 0x6d, 0x01})
	writeFile(t, dir, "notes.txt", []byte("ignored"))

	d := Dir{Path: dir}
	sources, err := d.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(sources) != 2 || sources[0].ID != "agent@1" || sources[1].ID != "beta" {
		t.Fatalf("sources = %+v", sources)
	}
	if len(sources[0].Interface) == 0 || len(sources[1].Interface) == 0 {
		t.Fatalf("interface sidecars not loaded: %+v", sources)
	}
	// No explicit default: the lexicographically first program.
	if id := d.DefaultID(); id != "agent@1" {
		t.Fatalf("default = %q", id)
	}
	if id := (Dir{Path: dir, Default: "beta"}).DefaultID(); id != "beta" {
		t.Fatalf("explicit default = %q", id)
	}
}

// A wasm without its interface sidecar is refused: a program directory must
// ship the manifest a caller reads to know what to pass.
func TestDirRequiresInterfaceSidecar(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "lonely.wasm", []byte{0x00, 0x61, 0x73, 0x6d})
	if _, err := (Dir{Path: dir}).List(context.Background()); err == nil {
		t.Fatal("expected an error for a wasm with no <id>.json interface")
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
