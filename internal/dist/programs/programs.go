// Package programs is the distribution's program registry: it loads program
// artifacts from a directory — each a <name>.wasm paired with its <name>.json
// interface manifest — and registers them with the runtime once, at boot.
// Reading the filesystem and decoding the manifest happen here: the runtime is
// handed prepared artifacts and never touches a disk.
package programs

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/aurora-capcompute/aurora-capcompute/aurora"
)

// Dir loads programs from a directory: every *.wasm file registers as a
// program whose id is the file name without the extension (so
// "agent@1.wasm" → "agent@1"), paired with its "<id>.json" interface manifest
// in the same directory. This is the boundary that decodes that manifest, so a
// sidecar that is missing or malformed is refused here, named, before the
// runtime sees the program.
type Dir struct {
	// Path is the directory scanned for *.wasm artifacts. Empty means no
	// programs.
	Path string
}

// interfaceManifest is the on-disk shape of a program's "<id>.json" sidecar. The
// runtime's own program type carries no serialization — how a program reaches it
// is this package's business — so the file format lives here.
type interfaceManifest struct {
	Description string          `json:"description"`
	Input       json.RawMessage `json:"input"`
	Output      json.RawMessage `json:"output"`
}

func (d Dir) List(_ context.Context) ([]aurora.ProgramData, error) {
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
	var programs []aurora.ProgramData
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".wasm") {
			continue
		}
		id := strings.TrimSuffix(entry.Name(), ".wasm")
		wasm, err := os.ReadFile(filepath.Join(d.Path, entry.Name()))
		if err != nil {
			return nil, fmt.Errorf("read program %s: %w", entry.Name(), err)
		}
		manifest, err := os.ReadFile(filepath.Join(d.Path, id+".json"))
		if err != nil {
			return nil, fmt.Errorf("read program %s interface (%s.json): %w", id, id, err)
		}
		var declared interfaceManifest
		if err := json.Unmarshal(manifest, &declared); err != nil {
			return nil, fmt.Errorf("decode program %s interface (%s.json): %w", id, id, err)
		}
		programs = append(programs, aurora.ProgramData{
			ID:          aurora.ProgramID(id),
			SourceCode:  wasm,
			Description: declared.Description,
			Input:       declared.Input,
			Output:      declared.Output,
		})
	}
	sort.Slice(programs, func(i, j int) bool { return programs[i].ID < programs[j].ID })
	return programs, nil
}

// Load registers every program in the directory with the runtime.
func (d Dir) Load(ctx context.Context, runtime aurora.Runtime) error {
	listed, err := d.List(ctx)
	if err != nil {
		return err
	}
	for _, program := range listed {
		if err := runtime.AddProgram(ctx, program); err != nil {
			return err
		}
	}
	return nil
}
