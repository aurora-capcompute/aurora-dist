package dist

import (
	"context"
	"encoding/json"

	"github.com/aurora-capcompute/aurora-capcompute/aurora"
	"github.com/aurora-capcompute/aurora-capcompute/capability"
	"github.com/aurora-capcompute/aurora-dispatchers/registry"
	"github.com/aurora-capcompute/aurora-dispatchers/scratch"
)

// provider adapts the dispatcher registry to the runtime's injected
// DispatcherProvider contract. It is the whole of the distribution's driver
// policy: the compiled-in registrations decide which tool types exist,
// services carry the deployment-scoped secret resolver and audit key, and the
// scratch spaces give each build core.scratch over its own session's store.
type provider struct {
	// registrations are the compiled-in drivers other than core.scratch, which
	// is registered per build over the session's store.
	registrations []registry.Registration
	services      registry.Services
	scratch       scratchSpaces
}

// scratchSpaces hands out one session's durable core.scratch store. The store
// is bound to the session, so the driver built over it cannot reach another.
type scratchSpaces interface {
	ScratchSpace(session string) scratch.Store
}

// drivers is the registry for one build: the compiled-in drivers and
// core.scratch over store. A config check has no process and passes nil.
func (p *provider) drivers(store scratch.Store) *registry.Registry {
	return registry.New(append([]registry.Registration{scratch.Registration{Store: store}}, p.registrations...)...)
}

// ValidateConfig refuses a grant's config at the door — at CreateProcess, so a
// bad manifest is a 400 rather than a process that dies on activation. It checks
// by building what the spawn would build and discarding it, which is why there
// is no second parser to keep in step with the real one.
func (p *provider) ValidateConfig(syscallType string, config json.RawMessage) error {
	return p.drivers(nil).ValidateConfig(context.Background(), syscallType, config, p.services)
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
	// The credential, never the manifest, names the session whose store
	// core.scratch gets.
	return p.drivers(p.scratch.ScratchSpace(cred.SessionID)).Build(ctx, entries, p.services)
}
