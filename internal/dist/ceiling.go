package dist

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/aurora-capcompute/aurora-capcompute/aurora"
	"github.com/aurora-capcompute/aurora-dispatchers/openaillm"
	"github.com/aurora-capcompute/capcompute/sys"
)

// The capability ceiling: a static, operator-configured list of capability
// names this deployment may ever grant. CreateProcess refuses manifests
// granting beyond it — sys.Attenuate at the door. It is defense in depth, not
// the reference monitor: the kernel's Validator still mediates every syscall
// against the per-process grant set; the ceiling merely guarantees the dist
// cannot exceed what its operator configured even if the (future) policy
// layer in front of it is compromised. An empty ceiling means unrestricted —
// the single-trusted-client posture.
type ceiling struct {
	allowed []sys.Capability
}

func newCeiling(names []string) *ceiling {
	if len(names) == 0 {
		return nil
	}
	allowed := make([]sys.Capability, 0, len(names))
	for _, name := range names {
		allowed = append(allowed, sys.Capability{Name: strings.TrimSpace(name)})
	}
	return &ceiling{allowed: allowed}
}

// check derives the capability names a manifest's composition would publish —
// for every node of the agent tree, since delegated children are granted at
// the same door — and verifies the whole set against the ceiling. The
// derivation mirrors what each compiled-in registration publishes; it is
// deliberately static (no MCP dial, no driver construction), which is why an
// open-ended MCP grant (no explicit tools list) cannot be bounded and is
// refused when a ceiling is configured.
func (c *ceiling) check(manifest aurora.Manifest) error {
	if c == nil {
		return nil
	}
	requested, err := grantedNames(manifest.Tools)
	if err != nil {
		return fmt.Errorf("%w: %v", aurora.ErrInvalid, err)
	}
	if _, err := sys.Attenuate(c.allowed, requested); err != nil {
		return fmt.Errorf("%w: capability ceiling: %v", aurora.ErrInvalid, err)
	}
	return nil
}

// grantedNames statically derives the capability names a tool list publishes,
// recursing through core.agent sub-trees. Names mirror each registration's
// publishing behavior:
//
//	core.internet, core.timer   → the tool's local name
//	core.memory                 → name.get, name.put, name.list
//	core.openaiApi              → the fixed openai.* operations
//	core.mcp                    → mcp.<server>.<tool> per explicit tools entry
//	core.agent                  → nothing external (delegation only)
func grantedNames(tools []aurora.Tool) ([]sys.Capability, error) {
	var out []sys.Capability
	add := func(names ...string) {
		for _, name := range names {
			out = append(out, sys.Capability{Name: name})
		}
	}
	for _, tool := range tools {
		switch tool.Type {
		case aurora.AgentToolType:
			nested, err := grantedNames(tool.Tools)
			if err != nil {
				return nil, err
			}
			out = append(out, nested...)
		case "core.internet", "core.timer":
			add(tool.Name)
		case "core.memory":
			add(tool.Name+".get", tool.Name+".put", tool.Name+".list")
		case openaillm.ToolType:
			add(openaillm.Operations()...)
		case "core.mcp":
			var settings struct {
				ServerID string   `json:"server_id"`
				Tools    []string `json:"tools"`
			}
			if len(tool.Settings) > 0 {
				if err := json.Unmarshal(tool.Settings, &settings); err != nil {
					return nil, fmt.Errorf("tool %q settings: %v", tool.Name, err)
				}
			}
			if len(settings.Tools) == 0 {
				return nil, fmt.Errorf("tool %q: an MCP grant without an explicit tools list cannot be bounded by the capability ceiling", tool.Name)
			}
			replacer := strings.NewReplacer(" ", "_", "/", "_", ":", "_")
			for _, name := range settings.Tools {
				add("mcp." + replacer.Replace(settings.ServerID) + "." + replacer.Replace(name))
			}
		default:
			// Unknown types fail manifest validation before the ceiling runs;
			// refuse here too so the ceiling stays conservative.
			return nil, fmt.Errorf("tool %q: type %q is not known to the capability ceiling", tool.Name, tool.Type)
		}
	}
	return out, nil
}
