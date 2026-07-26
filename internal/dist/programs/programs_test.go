package programs

import (
	"context"
	"os"
	"path/filepath"
	"strings"
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
	programs, err := d.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(programs) != 2 || programs[0].ID != "agent@1" || programs[1].ID != "beta" {
		t.Fatalf("programs = %+v", programs)
	}
	if len(programs[0].SourceCode) == 0 || len(programs[1].SourceCode) == 0 {
		t.Fatalf("wasm bytes not loaded: %+v", programs)
	}
	if programs[0].Description != "a program" || len(programs[0].Input) == 0 || len(programs[1].Output) == 0 {
		t.Fatalf("interface sidecars not decoded: %+v", programs)
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

// A sidecar that is not valid JSON is refused here, at the loader that decodes
// it — the runtime is handed a decoded interface and never sees the file.
func TestDirRejectsMalformedInterfaceSidecar(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "broken.wasm", []byte{0x00, 0x61, 0x73, 0x6d})
	writeFile(t, dir, "broken.json", []byte("not json"))
	_, err := (Dir{Path: dir}).List(context.Background())
	if err == nil {
		t.Fatal("expected an error for a malformed <id>.json interface")
	}
	if !strings.Contains(err.Error(), "broken.json") {
		t.Fatalf("error = %q, want the offending sidecar named", err)
	}
}

func TestDirToleratesMissingDirectory(t *testing.T) {
	d := Dir{Path: filepath.Join(t.TempDir(), "absent")}
	programs, err := d.List(context.Background())
	if err != nil || programs != nil {
		t.Fatalf("list = %v, %v", programs, err)
	}
}
