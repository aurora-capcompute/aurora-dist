// Package programs is the distribution's program registry: it loads wasm
// program artifacts from a directory and reconciles them into the runtime via
// SetPrograms (digest-diffed — unchanged programs keep running). The
// distribution re-scans the directory on a ticker so the runtime's in-memory
// program set tracks the filesystem.
package programs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/aurora-capcompute/aurora-capcompute/aurora"
)

// Dir loads programs from a directory: every *.wasm file registers as a
// program whose id is the file name without the extension (so
// "agent@1.wasm" → "agent@1"). It implements aurora.ProgramProvider for the
// initial load and re-scans on demand for hot reload.
type Dir struct {
	// Path is the directory scanned for *.wasm artifacts. Empty means no
	// programs — the runtime boots empty and gains programs on first reload.
	Path string
	// Default names the default program id. Empty falls back to the sole
	// program when exactly one is registered, else the lexicographically
	// first (mirroring the runtime's own default policy).
	Default string
}

func (d Dir) DefaultID() string {
	if d.Default != "" {
		return d.Default
	}
	sources, err := d.List(context.Background())
	if err != nil || len(sources) == 0 {
		return ""
	}
	ids := make([]string, 0, len(sources))
	for _, source := range sources {
		ids = append(ids, source.ID)
	}
	sort.Strings(ids)
	return ids[0]
}

func (d Dir) List(_ context.Context) ([]aurora.ProgramSource, error) {
	if strings.TrimSpace(d.Path) == "" {
		return nil, nil
	}
	entries, err := os.ReadDir(d.Path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("scan programs directory: %w", err)
	}
	var sources []aurora.ProgramSource
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".wasm") {
			continue
		}
		wasm, err := os.ReadFile(filepath.Join(d.Path, entry.Name()))
		if err != nil {
			return nil, fmt.Errorf("read program %s: %w", entry.Name(), err)
		}
		sources = append(sources, aurora.ProgramSource{
			ID:   strings.TrimSuffix(entry.Name(), ".wasm"),
			Wasm: wasm,
		})
	}
	sort.Slice(sources, func(i, j int) bool { return sources[i].ID < sources[j].ID })
	return sources, nil
}

// Reload re-scans the directory and reconciles the runtime's registered
// programs to it. Content-unchanged programs are left running.
func (d Dir) Reload(ctx context.Context, runtime aurora.Runtime) ([]aurora.ProgramArtifact, error) {
	sources, err := d.List(ctx)
	if err != nil {
		return nil, err
	}
	if err := runtime.SetPrograms(ctx, sources); err != nil {
		return nil, err
	}
	return runtime.Programs(), nil
}

