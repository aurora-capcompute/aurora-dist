package dist

import (
	"context"
	"encoding/json"
	"slices"
	"testing"

	"github.com/aurora-capcompute/aurora-capcompute/aurora"
	"github.com/aurora-capcompute/aurora-dispatchers/memory"
	"github.com/aurora-capcompute/aurora-dispatchers/registry"
	"github.com/aurora-capcompute/capcompute/sys"
)

// A manifest grant's data-flow policy must survive the whole assembly —
// provider → registry → driver — and be enforced at dispatch: each leaf grant
// publishes one capability named for its syscall, and a memory mount's `taints`
// refuse a write from a run that has observed the forbidden label (reads are not
// sinks, so the same mount serves get openly).
func TestProviderEnforcesMountFlow(t *testing.T) {
	provider := newProvider(
		[]registry.Registration{registry.InternetRegistration{}, registry.MemoryRegistration{}},
		registry.Services{Tenant: "acme", MemoryStore: memory.NewMapStore()},
	)
	manifest := aurora.Manifest{
		Version: aurora.ManifestVersion,
		Syscalls: []aurora.Syscall{
			{Syscall: "core.internet", Config: json.RawMessage(`{"capabilities":[{"methods":["GET"],"domain":"example.com","labels":["untrusted_web"]}]}`)},
			// The mount forbids untrusted_web: the write is guarded, the read is open.
			// A shared space needs no process identity, so the empty cred below builds.
			{Syscall: "core.memory", Config: json.RawMessage(`{"capabilities":[{"scope":"shared","space":"notes","operations":["get","put"],"taints":["untrusted_web"]}]}`)},
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

	// Per-mount flow policy reaches the published capability: the shared mount's
	// write forbids untrusted_web, its read does not, and the runtime's monitor
	// enforces that. Two mounts granting one operation would carry two policies
	// — which is why a case is (operation, scope, space) and not the operation
	// alone.
	var memoryCapability sys.Capability
	for _, capability := range dispatcher.Capabilities() {
		if capability.Name == "core.memory" {
			memoryCapability = capability
		}
	}
	write, ok := memoryCapability.FindOperation(
		json.RawMessage(`{"operation":"put","scope":"shared","space":"notes"}`))
	if !ok {
		t.Fatalf("the shared put case is not published: %+v", memoryCapability.Operations)
	}
	if !slices.Contains(write.Forbid, "untrusted_web") {
		t.Fatalf("SECURITY: shared put forbids %v, want untrusted_web", write.Forbid)
	}
	read, ok := memoryCapability.FindOperation(
		json.RawMessage(`{"operation":"get","scope":"shared","space":"notes"}`))
	if !ok {
		t.Fatalf("the shared get case is not published: %+v", memoryCapability.Operations)
	}
	if slices.Contains(read.Forbid, "untrusted_web") {
		t.Fatalf("the read is a source, not a sink: forbids %v", read.Forbid)
	}
}

// The process's identity rides the host credential, not the manifest, so the
// provider must thread each NewDispatcher's cred into that process's driver build
// for core.memory's session/process scopes to resolve to the right prefix. Two
// dispatchers over one shared store prove it: a session-scoped key written by one
// session is invisible to another (SessionID threaded and enforced), while a
// named shared space is the sanctioned crossing.
func TestProviderWiresCredentialIntoMemoryScopes(t *testing.T) {
	store := memory.NewMapStore()
	provider := newProvider(
		[]registry.Registration{registry.MemoryRegistration{}},
		registry.Services{Tenant: "acme", MemoryStore: store},
	)
	config := json.RawMessage(`{"capabilities":[
		{"scope":"session","operations":["get","put"]},
		{"scope":"shared","space":"team","operations":["get","put"]}
	]}`)
	manifest := aurora.Manifest{
		Version:  aurora.ManifestVersion,
		Syscalls: []aurora.Syscall{{Syscall: "core.memory", Config: config}},
	}

	build := func(session, process string) sys.Dispatcher[aurora.ProcessContext] {
		d, err := provider.NewDispatcher(context.Background(),
			aurora.ProcessContext{TenantID: "acme", SessionID: session, ProcessID: process}, manifest)
		if err != nil {
			t.Fatalf("new dispatcher %s/%s: %v", session, process, err)
		}
		return d
	}
	dispatch := func(d sys.Dispatcher[aurora.ProcessContext], args string) sys.SyscallResult {
		r, err := d.Dispatch(context.Background(), aurora.ProcessContext{},
			sys.Syscall{Name: "core.memory", Args: json.RawMessage(args)}, sys.Authorization{})
		if err != nil {
			t.Fatalf("dispatch %s: %v", args, err)
		}
		return r
	}
	found := func(r sys.SyscallResult) bool {
		if r.Status() != sys.StatusResult {
			t.Fatalf("op failed: %#v", r)
		}
		var resp memory.GetResponse
		if err := json.Unmarshal(r.Result(), &resp); err != nil {
			t.Fatalf("decode get: %v", err)
		}
		return resp.Found
	}

	one, two := build("s1", "p1"), build("s2", "p2")

	// A session-scoped write from s1 is private to s1's process tree.
	if r := dispatch(one, `{"operation":"put","scope":"session","key":"k","value":"s1"}`); r.Status() != sys.StatusResult {
		t.Fatalf("s1 session put = %#v", r)
	}
	if !found(dispatch(one, `{"operation":"get","scope":"session","key":"k"}`)) {
		t.Fatal("s1 could not read its own session key — the credential's SessionID was not threaded")
	}
	if found(dispatch(two, `{"operation":"get","scope":"session","key":"k"}`)) {
		t.Fatal("s2 read s1's session key — session scopes collapsed, identity ignored")
	}

	// A named shared space is the sanctioned crossing between the two sessions.
	if r := dispatch(one, `{"operation":"put","scope":"shared","space":"team","key":"k","value":"shared"}`); r.Status() != sys.StatusResult {
		t.Fatalf("s1 shared put = %#v", r)
	}
	if !found(dispatch(two, `{"operation":"get","scope":"shared","space":"team","key":"k"}`)) {
		t.Fatal("named shared space did not cross sessions")
	}
}
