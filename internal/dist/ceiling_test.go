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
	c := newCeiling([]string{"internet.read", "timer.set", "memory.get", "memory.put", "memory.list"})

	ok := manifestWith(
		aurora.Syscall{Syscall: "core.internet"},
		aurora.Syscall{Syscall: "core.timer"},
		aurora.Syscall{Syscall: "core.memory"},
	)
	if err := c.check(ok); err != nil {
		t.Fatalf("within ceiling rejected: %v", err)
	}

	over := manifestWith(aurora.Syscall{Syscall: "core.internet"})
	err := newCeiling([]string{"timer.set"}).check(over)
	if err == nil || !errors.Is(err, aurora.ErrInvalid) {
		t.Fatalf("beyond ceiling = %v, want ErrInvalid", err)
	}
	if !strings.Contains(err.Error(), "internet.read") {
		t.Fatalf("error does not name the violating capability: %v", err)
	}
}

func TestCeilingSeesThroughSpawnTrees(t *testing.T) {
	c := newCeiling([]string{"internet.read"})
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

func TestCeilingRefusesOpenEndedMCP(t *testing.T) {
	c := newCeiling([]string{"mcp.docs.search"})
	explicit := manifestWith(aurora.Syscall{
		Syscall:  "core.mcp",
		Settings: json.RawMessage(`{"server_id":"docs","tools":["search"]}`),
	})
	if err := c.check(explicit); err != nil {
		t.Fatalf("explicit MCP tools rejected: %v", err)
	}
	open := manifestWith(aurora.Syscall{
		Syscall:  "core.mcp",
		Settings: json.RawMessage(`{"server_id":"docs"}`),
	})
	if err := c.check(open); err == nil {
		t.Fatal("an open-ended MCP grant cannot be bounded and must be refused")
	}
}
