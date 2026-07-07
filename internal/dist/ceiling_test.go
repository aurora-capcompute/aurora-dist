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
	c := newCeiling([]string{"internet.fetch", "sys.timer", "memory.get", "memory.put", "memory.list"})

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
	if !strings.Contains(err.Error(), "internet.fetch") {
		t.Fatalf("error does not name the violating capability: %v", err)
	}
}

func TestCeilingSeesThroughSpawnTrees(t *testing.T) {
	c := newCeiling([]string{"internet.fetch"})
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

func TestCeilingCoversFixedOpenAIOperations(t *testing.T) {
	c := newCeiling([]string{"openai.chat", "openai.responses", "openai.embeddings", "openai.models.list"})
	if err := c.check(manifestWith(aurora.Syscall{Syscall: "core.openaiApi", Hidden: true})); err != nil {
		t.Fatalf("openai grants rejected: %v", err)
	}
	if err := newCeiling([]string{"openai.chat"}).check(manifestWith(aurora.Syscall{Syscall: "core.openaiApi"})); err == nil {
		t.Fatal("partial openai ceiling must refuse the full grant")
	}
}

// MCP is dropped: its grants fall to the unknown-syscall refusal, keeping
// the ceiling conservative.
func TestCeilingRefusesUnknownSyscalls(t *testing.T) {
	c := newCeiling([]string{"sys.timer"})
	if err := c.check(manifestWith(aurora.Syscall{Syscall: "core.mcp",
		Settings: json.RawMessage(`{"server_id":"docs"}`)})); err == nil {
		t.Fatal("an unknown syscall must be refused by the ceiling")
	}
}
