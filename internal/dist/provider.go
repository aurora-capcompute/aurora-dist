package dist

import (
	"context"
	"encoding/json"

	"github.com/aurora-capcompute/aurora-capcompute/aurora"
	"github.com/aurora-capcompute/aurora-capcompute/capability"
	"github.com/aurora-capcompute/aurora-dispatchers/registry"
)

// provider adapts the dispatcher registry to the runtime's injected
// DispatcherProvider contract. It is the whole of the distribution's driver
// policy: the compiled-in registration set decides which tool types exist,
// and services carry the deployment-scoped backends (the tenant memory store).
// There is no per-binding warmup or secret resolution here — manifests arrive
// per-process from the single trusted client, already carrying their driver
// config; the policy layer in front of multi-principal deployments is a
// separate service (D3).
type provider struct {
	registry *registry.Registry
	services registry.Services
}

func newProvider(registrations []registry.Registration, services registry.Services) *provider {
	return &provider{registry: registry.New(registrations...), services: services}
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
	return p.registry.Build(ctx, entries, p.services)
}
