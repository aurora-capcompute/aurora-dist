package dist

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/aurora-capcompute/aurora-capcompute/aurora"
	"github.com/aurora-capcompute/aurora-dispatchers/memory"
	"github.com/aurora-capcompute/aurora-dispatchers/registry"
	"github.com/aurora-capcompute/capcompute/sys"
)

// A manifest grant's labels/forbid must reach the live capabilities the kernel's
// provenance monitor reads: source labels on the emitting capability, the forbid
// set on every capability a multi-op grant publishes.
func TestProviderAppliesGrantDataFlowPolicy(t *testing.T) {
	provider := newProvider(
		[]registry.Registration{registry.InternetRegistration{}, registry.MemoryRegistration{}},
		registry.Services{Tenant: "acme", MemoryStore: memory.NewMapStore()},
	)
	manifest := aurora.Manifest{
		Version: aurora.ManifestVersion,
		Syscalls: []aurora.Syscall{
			{
				Syscall:  "core.internet",
				Settings: json.RawMessage(`{"permissions":[{"methods":["GET"],"domain":"example.com"}]}`),
				Labels:   map[string][]string{"*": {"untrusted_web"}},
			},
			// Per-operation targeting: forbid the write, leave the read alone.
			{Syscall: "core.memory", Forbid: map[string][]string{"memory.put": {"untrusted_web"}}},
		},
	}
	dispatcher, err := provider.NewDispatcher(context.Background(), aurora.ProcessContext{}, manifest)
	if err != nil {
		t.Fatalf("new dispatcher: %v", err)
	}

	byName := map[string]sys.Capability{}
	for _, capability := range dispatcher.Capabilities() {
		byName[capability.Name] = capability
	}
	if web := byName["net.http"]; len(web.Labels) != 1 || web.Labels[0] != "untrusted_web" {
		t.Fatalf("net.http labels = %v, want [untrusted_web]", web.Labels)
	}
	if put := byName["memory.put"]; len(put.Forbid) != 1 || put.Forbid[0] != "untrusted_web" {
		t.Fatalf("memory.put forbid = %v, want [untrusted_web]", put.Forbid)
	}
	if get := byName["memory.get"]; len(get.Forbid) != 0 {
		t.Fatalf("memory.get forbid = %v, want none (per-operation targeting)", get.Forbid)
	}
}
