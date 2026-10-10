package dist

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/aurora-capcompute/aurora-capcompute/aurora"
	"github.com/aurora-capcompute/aurora-dispatchers/internet"
	"github.com/aurora-capcompute/aurora-dispatchers/openaillm"
	"github.com/aurora-capcompute/aurora-dispatchers/registry"
	"github.com/aurora-capcompute/aurora-dist/internal/dist/skills"
)

func TestValidateManifestRefusesAtBoot(t *testing.T) {
	provider := &provider{registrations: []registry.Registration{internet.Registration{}}}
	programs := []aurora.ProgramData{{ID: "agent"}}
	grant := func(syscall, config string) aurora.Manifest {
		m := aurora.Manifest{Version: aurora.ManifestVersion, Program: "agent"}
		m.Syscalls = []aurora.Syscall{{Syscall: syscall, Config: json.RawMessage(config)}}
		return m
	}

	valid := grant("core.internet", `{"capabilities":[{"methods":["GET"],"domain":"status.example.com"}]}`)
	if _, err := validateManifest(valid, programs, provider); err != nil {
		t.Fatalf("valid manifest refused: %v", err)
	}

	unknownProgram := valid
	unknownProgram.Program = "other"
	for name, tc := range map[string]struct {
		manifest aurora.Manifest
		want     string
	}{
		"no manifest":     {aurora.Manifest{}, "version"},
		"unknown program": {unknownProgram, "not in the programs directory"},
		"reserved":        {grant("sys.input", ""), "reserved"},
		"bad config":      {grant("core.internet", `{"capabilities":[{"methods":["GET"],"domain":""}]}`), "core.internet"},
		"unknown driver":  {grant("core.nothing", `{}`), "core.nothing"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := validateManifest(tc.manifest, programs, provider)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want it to mention %q", err, tc.want)
			}
		})
	}
}

// testdata/skills holds the README's example skills.
func TestExampleSkillsBoot(t *testing.T) {
	loaded, err := skills.Load("testdata/skills")
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := skills.Merge("agent", loaded)
	if err != nil {
		t.Fatal(err)
	}
	provider := &provider{
		registrations: []registry.Registration{internet.Registration{}, openaillm.Registration{}},
		services:      registry.Services{Secrets: secretMap{"OPENAI_KEY": "sk-test"}},
	}
	if _, err := validateManifest(manifest, []aurora.ProgramData{{ID: "agent"}}, provider); err != nil {
		t.Fatal(err)
	}
	if len(manifest.Skills) != 2 || manifest.Skills[1].Instructions == "" {
		t.Fatalf("skills = %+v", manifest.Skills)
	}
}

type secretMap map[string]string

func (m secretMap) Resolve(name string) (string, bool) { v, ok := m[name]; return v, ok }
