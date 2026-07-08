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

// A manifest grant's per-operation data-flow policy must survive the whole
// assembly — provider → registry → driver — and be enforced at dispatch: each
// leaf grant publishes one capability named for its syscall, and a granted
// operation's `taints` refuse a run that has observed the forbidden label.
func TestProviderEnforcesPerOperationFlow(t *testing.T) {
	provider := newProvider(
		[]registry.Registration{registry.InternetRegistration{}, registry.MemoryRegistration{}},
		registry.Services{Tenant: "acme", MemoryStore: memory.NewMapStore()},
	)
	manifest := aurora.Manifest{
		Version: aurora.ManifestVersion,
		Syscalls: []aurora.Syscall{
			{Syscall: "core.internet", Config: json.RawMessage(`{"capabilities":[{"methods":["GET"],"domain":"example.com","labels":["untrusted_web"]}]}`)},
			// Per-operation targeting: the write forbids untrusted_web, the read is open.
			{Syscall: "core.memory", Config: json.RawMessage(`{"capabilities":[{"operation":"get"},{"operation":"put","taints":["untrusted_web"]}]}`)},
		},
	}
	dispatcher, err := provider.NewDispatcher(context.Background(), aurora.ProcessContext{}, manifest)
	if err != nil {
		t.Fatalf("new dispatcher: %v", err)
	}

	// Each leaf grant publishes exactly one capability, named for its syscall.
	names := map[string]bool{}
	for _, capability := range dispatcher.Capabilities() {
		names[capability.Name] = true
	}
	for _, want := range []string{"core.internet", "core.memory"} {
		if !names[want] {
			t.Fatalf("capability %q not published: %v", want, names)
		}
	}

	put := func(ctx context.Context) sys.SyscallResult {
		result, err := dispatcher.Dispatch(ctx, aurora.ProcessContext{}, sys.Syscall{
			Name: "core.memory", Args: json.RawMessage(`{"operation":"put","key":"k","value":"v"}`)}, sys.Authorization{})
		if err != nil {
			t.Fatalf("dispatch put: %v", err)
		}
		return result
	}
	// A run tainted with untrusted_web may not write.
	if blocked := put(sys.WithTaint(context.Background(), []string{"untrusted_web"})); blocked.Status() != sys.StatusFailed || blocked.Errno() != sys.ErrnoDenied {
		t.Fatalf("tainted put = %v/%v, want failed/denied", blocked.Status(), blocked.Errno())
	}
	// A clean run may.
	if ok := put(context.Background()); ok.Status() != sys.StatusResult {
		t.Fatalf("clean put = %v", ok.Status())
	}
}
