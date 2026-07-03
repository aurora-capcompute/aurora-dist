// Package programs is the distribution's program registry: it loads wasm
// program artifacts from a directory, hot-reloads them into the runtime via
// SetPrograms (digest-diffed — unchanged programs keep running), and answers
// the retention query that gates decommissioning: which program digests are
// still referenced by non-terminal processes. Upgrades are drain-and-
// deprecate — new processes bind the new digest, parked ones drain, and an
// artifact may be removed only once nothing non-terminal references it.
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

// Reference is one digest's retention state: the registered program ids
// carrying it and the non-terminal processes still pinned to it. A digest is
// decommissionable exactly when no non-terminal process references it.
type Reference struct {
	Digest string `json:"digest"`
	// Programs lists registered program ids whose current artifact carries
	// this digest (empty for a digest only historical processes reference).
	Programs []string `json:"programs,omitempty"`
	// Processes lists non-terminal process ids pinned to this digest.
	Processes []string `json:"processes,omitempty"`
	// Decommissionable is true when no non-terminal process references the
	// digest — the artifact may be removed without stranding a resume.
	Decommissionable bool `json:"decommissionable"`
}

// Retention projects the retention query over current run state: every digest
// that is registered or referenced, with the non-terminal processes pinning
// it. Terminal = completed, failed, or stopped; an interrupted or parked
// process is resumable and keeps its digest alive.
func Retention(runtime aurora.Runtime) []Reference {
	refs := map[string]*Reference{}
	ref := func(digest string) *Reference {
		if digest == "" {
			return nil
		}
		if r, ok := refs[digest]; ok {
			return r
		}
		r := &Reference{Digest: digest}
		refs[digest] = r
		return r
	}
	for _, artifact := range runtime.Programs() {
		if r := ref(artifact.Digest); r != nil {
			r.Programs = append(r.Programs, artifact.ID)
		}
	}
	for _, summary := range runtime.ListSessions() {
		session, err := runtime.GetSession(summary.ID)
		if err != nil {
			continue
		}
		for _, process := range session.Processes {
			switch process.Status {
			case aurora.ProcessCompleted, aurora.ProcessFailed, aurora.ProcessStopped:
				continue
			}
			if r := ref(process.ProgramDigest); r != nil {
				r.Processes = append(r.Processes, process.ID)
			}
		}
	}
	out := make([]Reference, 0, len(refs))
	for _, r := range refs {
		sort.Strings(r.Programs)
		sort.Strings(r.Processes)
		r.Decommissionable = len(r.Processes) == 0
		out = append(out, *r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Digest < out[j].Digest })
	return out
}
