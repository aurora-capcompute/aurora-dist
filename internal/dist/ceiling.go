package dist

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/aurora-capcompute/aurora-capcompute/aurora"
	"github.com/aurora-capcompute/aurora-dispatchers/internet"
	"github.com/aurora-capcompute/aurora-dispatchers/memory"
	"github.com/aurora-capcompute/aurora-dispatchers/openaillm"
	"github.com/aurora-capcompute/aurora-dispatchers/timer"
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

// check derives the capability names a manifest's grant set would publish —
// for every node of the spawn tree, since spawned children are granted at
// the same door — and verifies the whole set against the ceiling. The
// derivation mirrors what each compiled-in registration publishes; it is
// deliberately static (no MCP dial, no driver construction), which is why an
// open-ended MCP grant (no explicit tools list) cannot be bounded and is
// refused when a ceiling is configured.
func (c *ceiling) check(manifest aurora.Manifest) error {
	if c == nil {
		return nil
	}
	requested, err := grantedNames(manifest.Syscalls)
	if err != nil {
		return fmt.Errorf("%w: %v", aurora.ErrInvalid, err)
	}
	if _, err := sys.Attenuate(c.allowed, requested); err != nil {
		return fmt.Errorf("%w: capability ceiling: %v", aurora.ErrInvalid, err)
	}
	return nil
}

// grantedNames statically derives the capability names a grant set publishes,
// recursing through core.spawn subtrees. Names mirror each registration's
// canonical publishing behavior:
//
//	core.timer                  → timer.set
//	core.internet               → internet.read
//	core.memory                 → memory.get, memory.put, memory.list
//	core.openaiApi              → the fixed openai.* operations
//	core.mcp                    → mcp.<server>.<tool> per explicit tools entry
//	core.spawn                  → nothing external (each spawnable program is
//	                              granted at the same door, recursively)
func grantedNames(syscalls []aurora.Syscall) ([]sys.Capability, error) {
	var out []sys.Capability
	add := func(names ...string) {
		for _, name := range names {
			out = append(out, sys.Capability{Name: name})
		}
	}
	for _, grant := range syscalls {
		switch grant.Syscall {
		case aurora.SpawnSyscall:
			for _, program := range grant.Programs {
				nested, err := grantedNames(program.Syscalls)
				if err != nil {
					return nil, err
				}
				out = append(out, nested...)
			}
		case "core.timer":
			add(timer.Capability)
		case "core.internet":
			add(internet.Capability)
		case "core.memory":
			add(memory.Capability+".get", memory.Capability+".put", memory.Capability+".list")
		case openaillm.SyscallType:
			add(openaillm.Operations()...)
		case "core.mcp":
			var settings struct {
				ServerID string   `json:"server_id"`
				Tools    []string `json:"tools"`
			}
			if len(grant.Settings) > 0 {
				if err := json.Unmarshal(grant.Settings, &settings); err != nil {
					return nil, fmt.Errorf("syscall %q settings: %v", grant.Syscall, err)
				}
			}
			if len(settings.Tools) == 0 {
				return nil, errors.New("an MCP grant without an explicit tools list cannot be bounded by the capability ceiling")
			}
			replacer := strings.NewReplacer(" ", "_", "/", "_", ":", "_")
			for _, name := range settings.Tools {
				add("mcp." + replacer.Replace(settings.ServerID) + "." + replacer.Replace(name))
			}
		default:
			// Unknown syscalls fail manifest validation before the ceiling
			// runs; refuse here too so the ceiling stays conservative.
			return nil, fmt.Errorf("syscall %q is not known to the capability ceiling", grant.Syscall)
		}
	}
	return out, nil
}
