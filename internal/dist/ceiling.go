package dist

import (
	"fmt"
	"strings"

	"github.com/aurora-capcompute/aurora-capcompute/aurora"
	"github.com/aurora-capcompute/aurora-dispatchers/internet"
	"github.com/aurora-capcompute/aurora-dispatchers/memory"
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
// recursing through sys.spawn subtrees. Each grant publishes exactly one
// capability, named for its syscall — its operations are cases of that one
// capability's ADT, not separate names — so the ceiling gates families, not
// individual operations (a manifest's `capabilities` list selects operations
// within a granted family):
//
//	sys.timer                   → sys.timer (the runtime's own)
//	core.internet               → core.internet
//	core.memory                 → core.memory
//	core.openaiApi              → core.openaiApi
//	sys.spawn                   → nothing external (each spawnable program is
//	                              granted at the same door, recursively)
func grantedNames(syscalls []aurora.Syscall) ([]sys.Capability, error) {
	var out []sys.Capability
	add := func(name string) { out = append(out, sys.Capability{Name: name}) }
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
		case aurora.TimerSyscall:
			add(aurora.TimerSyscall)
		case internet.Capability:
			add(internet.Capability)
		case memory.Capability:
			add(memory.Capability)
		case openaillm.SyscallType:
			add(openaillm.SyscallType)
		default:
			// Unknown syscalls fail manifest validation before the ceiling
			// runs; refuse here too so the ceiling stays conservative.
			return nil, fmt.Errorf("syscall %q is not known to the capability ceiling", grant.Syscall)
		}
	}
	return out, nil
}
