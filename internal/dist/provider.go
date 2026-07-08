package dist

import (
	"context"
	"encoding/json"

	"github.com/aurora-capcompute/aurora-capcompute/aurora"
	"github.com/aurora-capcompute/aurora-dispatchers/builtin"
	"github.com/aurora-capcompute/aurora-dispatchers/registry"
	"github.com/aurora-capcompute/capcompute/sys"
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

func (p *provider) Normalize(syscallType string, config json.RawMessage) (json.RawMessage, error) {
	return p.registry.Normalize(syscallType, config)
}

func (p *provider) NewDispatcher(
	ctx context.Context,
	_ aurora.ProcessContext,
	manifest aurora.Manifest,
) (sys.Dispatcher[aurora.ProcessContext], error) {
	leaf := manifest.LeafSyscalls()
	entries := make([]registry.Entry, 0, len(leaf))
	for _, grant := range leaf {
		entries = append(entries, registry.Entry{
			Syscall: grant.Syscall, Config: grant.Config, Hidden: grant.Hidden,
		})
	}
	config, err := p.registry.Build(ctx, entries, p.services)
	if err != nil {
		return nil, err
	}
	return builtin.New[aurora.ProcessContext](config), nil
}
