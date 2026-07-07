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
				Labels:   []string{"untrusted_web"},
			},
			{Syscall: "core.memory", Forbid: []string{"untrusted_web"}},
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
	for _, op := range []string{"memory.get", "memory.put", "memory.list"} {
		got := byName[op]
		if len(got.Forbid) != 1 || got.Forbid[0] != "untrusted_web" {
			t.Fatalf("%s forbid = %v, want [untrusted_web]", op, got.Forbid)
		}
	}
}
