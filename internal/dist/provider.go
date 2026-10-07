package dist

import (
	"context"
	"encoding/json"

	"github.com/aurora-capcompute/aurora-capcompute/aurora"
	"github.com/aurora-capcompute/aurora-capcompute/capability"
	drivermem "github.com/aurora-capcompute/aurora-dispatchers/memory"
	"github.com/aurora-capcompute/aurora-dispatchers/registry"
)

// provider adapts the dispatcher registry to the runtime's injected
// DispatcherProvider contract. It is the whole of the distribution's driver
// policy: the compiled-in registration set decides which tool types exist,
// services carry the deployment-scoped secret resolver and audit key, and the
// scratch spaces give each build its session's core.scratch store.
// There is no per-binding warmup here — manifests arrive
// per-process from the single trusted client, already carrying their driver
// config; the policy layer in front of multi-principal deployments is a
// separate service (D3).
type provider struct {
	registry *registry.Registry
	services registry.Services
	scratch  scratchSpaces
}

// scratchSpaces hands out one session's durable core.scratch store. The store
// is bound to the session, so the driver built over it cannot reach another.
type scratchSpaces interface {
	ScratchSpace(session string) drivermem.Store
}

func newProvider(registrations []registry.Registration, services registry.Services, scratch scratchSpaces) *provider {
	return &provider{registry: registry.New(registrations...), services: services, scratch: scratch}
}

// ValidateConfig refuses a grant's config at the door — at CreateProcess, so a
// bad manifest is a 400 rather than a process that dies on activation. It checks
// by building what the spawn would build and discarding it, which is why there
// is no second parser to keep in step with the real one.
func (p *provider) ValidateConfig(syscallType string, config json.RawMessage) error {
	return p.registry.ValidateConfig(context.Background(), syscallType, config, p.services)
}

func (p *provider) NewDispatcher(
	ctx context.Context,
	cred aurora.ProcessContext,
	manifest aurora.Manifest,
) (*capability.Table, error) {
	leaf := manifest.LeafSyscalls()
	entries := make([]registry.Entry, 0, len(leaf))
	for _, grant := range leaf {
		entries = append(entries, registry.Entry{
			Syscall: grant.Syscall, Config: grant.Config, Hidden: grant.Hidden,
		})
	}
	// The scratch space is the one per-process service: the credential, never
	// the manifest, names the session whose store the build gets.
	services := p.services
	services.Scratch = p.scratch.ScratchSpace(cred.SessionID)
	return p.registry.Build(ctx, entries, services)
}
