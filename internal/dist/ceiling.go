package dist

import (
	"fmt"
	"sort"
	"strings"

	"github.com/aurora-capcompute/aurora-capcompute/aurora"
	"github.com/aurora-capcompute/aurora-dispatchers/command"
	"github.com/aurora-capcompute/aurora-dispatchers/filesystem"
	"github.com/aurora-capcompute/aurora-dispatchers/internet"
	"github.com/aurora-capcompute/aurora-dispatchers/memory"
	"github.com/aurora-capcompute/aurora-dispatchers/openaillm"
	"github.com/aurora-capcompute/aurora-dispatchers/registry"
	"github.com/aurora-capcompute/capcompute/sys"
)

// The capability ceiling: a static, operator-configured list of capability
// names this deployment may ever grant. CreateProcess refuses manifests
// granting beyond it — monitor.Attenuate at the door. It is defense in depth, not
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
// deliberately static (no driver construction), which it can be because every
// grant publishes exactly one capability named for its syscall (grantedNames),
// so the whole set is enumerable without instantiating anything.
func (c *ceiling) check(manifest aurora.Manifest) error {
	if c == nil {
		return nil
	}
	requested, err := grantedNames(manifest.Syscalls)
	if err != nil {
		return fmt.Errorf("%w: %v", aurora.ErrInvalid, err)
	}
	allowed := make(map[string]struct{}, len(c.allowed))
	for _, capability := range c.allowed {
		allowed[capability.Name] = struct{}{}
	}
	var refused []string
	for _, capability := range requested {
		if _, ok := allowed[capability.Name]; !ok {
			refused = append(refused, capability.Name)
		}
	}
	if len(refused) > 0 {
		sort.Strings(refused)
		return fmt.Errorf("%w: capability ceiling: %s not permitted",
			aurora.ErrInvalid, strings.Join(refused, ", "))
	}
	return nil
}

// grantedNames statically derives the capability names a grant set publishes.
// Each grant publishes exactly one
// capability, named for its syscall — its operations are cases of that one
// capability's ADT, not separate names — so the ceiling gates families, not
// individual operations (a manifest's `capabilities` list selects operations
// within a granted family):
//
//	sys.timer                   → sys.timer (the runtime's own)
//	core.internet               → core.internet
//	core.memory                 → core.memory
//	core.scratch                → core.scratch
//	core.filesystem             → core.filesystem
//	core.openaiApi              → core.openaiApi
func grantedNames(syscalls []aurora.Syscall) ([]sys.Capability, error) {
	var out []sys.Capability
	add := func(name string) { out = append(out, sys.Capability{Name: name}) }
	for _, grant := range syscalls {
		switch grant.Syscall {
		case aurora.TimerSyscall:
			add(aurora.TimerSyscall)
		case internet.Capability:
			add(internet.Capability)
		case memory.Capability:
			add(memory.Capability)
		case registry.ScratchCapability:
			add(registry.ScratchCapability)
		case filesystem.Capability:
			add(filesystem.Capability)
		case command.Capability:
			add(command.Capability)
		case registry.HTTPTemplateSyscall:
			add(registry.HTTPTemplateSyscall)
		case openaillm.SyscallType:
			add(openaillm.SyscallType)
		case aurora.DeclassifySyscall:
			// sys.declassify is a valid runtime-served grant (like sys.timer); it
			// must be gateable by the ceiling, not fall to the unknown-syscall
			// refusal, which would break every manifest that grants it whenever a
			// ceiling is set.
			add(aurora.DeclassifySyscall)
		default:
			// Unknown syscalls fail manifest validation before the ceiling
			// runs; refuse here too so the ceiling stays conservative.
			return nil, fmt.Errorf("syscall %q is not known to the capability ceiling", grant.Syscall)
		}
	}
	return out, nil
}
