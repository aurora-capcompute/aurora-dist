package dist

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/aurora-capcompute/aurora-capcompute/aurora"
)

func manifestWith(tools ...aurora.Tool) aurora.Manifest {
	return aurora.Manifest{Version: aurora.ManifestVersion, Tools: tools}
}

func TestCeilingNilAllowsEverything(t *testing.T) {
	var c *ceiling
	if err := c.check(manifestWith(aurora.Tool{Name: "fetch", Type: "core.internet"})); err != nil {
		t.Fatalf("nil ceiling rejected: %v", err)
	}
}

func TestCeilingAttenuatesGrants(t *testing.T) {
	c := newCeiling([]string{"fetch", "timer.set", "mem.get", "mem.put", "mem.list"})

	ok := manifestWith(
		aurora.Tool{Name: "fetch", Type: "core.internet"},
		aurora.Tool{Name: "timer.set", Type: "core.timer"},
		aurora.Tool{Name: "mem", Type: "core.memory"},
	)
	if err := c.check(ok); err != nil {
		t.Fatalf("within ceiling rejected: %v", err)
	}

	over := manifestWith(aurora.Tool{Name: "other", Type: "core.internet"})
	err := c.check(over)
	if err == nil || !errors.Is(err, aurora.ErrInvalid) {
		t.Fatalf("beyond ceiling = %v, want ErrInvalid", err)
	}
	if !strings.Contains(err.Error(), "other") {
		t.Fatalf("error does not name the violating capability: %v", err)
	}
}

func TestCeilingSeesThroughAgentTrees(t *testing.T) {
	c := newCeiling([]string{"fetch"})
	nested := manifestWith(aurora.Tool{
		Name: "child", Type: aurora.AgentToolType,
		Settings: json.RawMessage(`{"program":"p"}`),
		Tools:    []aurora.Tool{{Name: "timer.set", Type: "core.timer"}},
	})
	if err := c.check(nested); err == nil {
		t.Fatal("a child grant beyond the ceiling must be refused at the door")
	}
}

func TestCeilingCoversFixedOpenAIOperations(t *testing.T) {
	c := newCeiling([]string{"openai.chat", "openai.responses", "openai.embeddings", "openai.models.list"})
	if err := c.check(manifestWith(aurora.Tool{Name: "llm", Type: "core.openaiApi", Hidden: true})); err != nil {
		t.Fatalf("openai grants rejected: %v", err)
	}
	if err := newCeiling([]string{"openai.chat"}).check(manifestWith(aurora.Tool{Name: "llm", Type: "core.openaiApi"})); err == nil {
		t.Fatal("partial openai ceiling must refuse the full grant")
	}
}

func TestCeilingRefusesOpenEndedMCP(t *testing.T) {
	c := newCeiling([]string{"mcp.docs.search"})
	explicit := manifestWith(aurora.Tool{
		Name: "docs", Type: "core.mcp",
		Settings: json.RawMessage(`{"server_id":"docs","tools":["search"]}`),
	})
	if err := c.check(explicit); err != nil {
		t.Fatalf("explicit MCP tools rejected: %v", err)
	}
	open := manifestWith(aurora.Tool{
		Name: "docs", Type: "core.mcp",
		Settings: json.RawMessage(`{"server_id":"docs"}`),
	})
	if err := c.check(open); err == nil {
		t.Fatal("an open-ended MCP grant cannot be bounded and must be refused")
	}
}
