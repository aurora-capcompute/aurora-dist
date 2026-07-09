package dist

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/aurora-capcompute/aurora-capcompute/aurora"
)

func manifestWith(syscalls ...aurora.Syscall) aurora.Manifest {
	return aurora.Manifest{Version: aurora.ManifestVersion, Syscalls: syscalls}
}

func TestCeilingNilAllowsEverything(t *testing.T) {
	var c *ceiling
	if err := c.check(manifestWith(aurora.Syscall{Syscall: "core.internet"})); err != nil {
		t.Fatalf("nil ceiling rejected: %v", err)
	}
}

func TestCeilingAttenuatesGrants(t *testing.T) {
	c := newCeiling([]string{"core.internet", "sys.timer", "core.memory"})

	ok := manifestWith(
		aurora.Syscall{Syscall: "core.internet"},
		aurora.Syscall{Syscall: "sys.timer"},
		aurora.Syscall{Syscall: "core.memory"},
	)
	if err := c.check(ok); err != nil {
		t.Fatalf("within ceiling rejected: %v", err)
	}

	over := manifestWith(aurora.Syscall{Syscall: "core.internet"})
	err := newCeiling([]string{"sys.timer"}).check(over)
	if err == nil || !errors.Is(err, aurora.ErrInvalid) {
		t.Fatalf("beyond ceiling = %v, want ErrInvalid", err)
	}
	if !strings.Contains(err.Error(), "core.internet") {
		t.Fatalf("error does not name the violating capability: %v", err)
	}
}

func TestCeilingSeesThroughSpawnTrees(t *testing.T) {
	c := newCeiling([]string{"core.internet"})
	nested := manifestWith(aurora.Syscall{
		Syscall: aurora.SpawnSyscall,
		Programs: []aurora.Manifest{{
			Program:  "p",
			Syscalls: []aurora.Syscall{{Syscall: "core.timer"}},
		}},
	})
	if err := c.check(nested); err == nil {
		t.Fatal("a spawnable program's grant beyond the ceiling must be refused at the door")
	}
}

// A leaf grant publishes one capability, named for its syscall: the ceiling
// gates the family, and operations are selected within the manifest.
func TestCeilingGatesOpenAIFamily(t *testing.T) {
	c := newCeiling([]string{"core.openaiApi"})
	if err := c.check(manifestWith(aurora.Syscall{Syscall: "core.openaiApi", Hidden: true})); err != nil {
		t.Fatalf("openai grant rejected: %v", err)
	}
	if err := newCeiling([]string{"core.internet"}).check(manifestWith(aurora.Syscall{Syscall: "core.openaiApi"})); err == nil {
		t.Fatal("a ceiling without core.openaiApi must refuse the grant")
	}
}

// MCP is dropped: its grants fall to the unknown-syscall refusal, keeping
// the ceiling conservative.
func TestCeilingRefusesUnknownSyscalls(t *testing.T) {
	c := newCeiling([]string{"sys.timer"})
	if err := c.check(manifestWith(aurora.Syscall{Syscall: "core.mcp",
		Config: json.RawMessage(`{"server_id":"docs"}`)})); err == nil {
		t.Fatal("an unknown syscall must be refused by the ceiling")
	}
}

// The ceiling must recognize every capability the distribution's registry can
// build — filesystem and scratch included — or a non-empty ceiling silently
// fail-closes grants the operator explicitly listed (a drift bug, not a leak).
func TestCeilingGatesFilesystemAndScratch(t *testing.T) {
	c := newCeiling([]string{"core.filesystem", "core.scratch"})
	granted := manifestWith(
		aurora.Syscall{Syscall: "core.filesystem"},
		aurora.Syscall{Syscall: "core.scratch"},
	)
	if err := c.check(granted); err != nil {
		t.Fatalf("a ceiling listing filesystem/scratch must admit their grants: %v", err)
	}
	// A ceiling that omits them still refuses (the gate stays real).
	if err := newCeiling([]string{"core.memory"}).check(
		manifestWith(aurora.Syscall{Syscall: "core.filesystem"})); err == nil {
		t.Fatal("a ceiling without core.filesystem must refuse the grant")
	}
}
