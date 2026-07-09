package dist

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/aurora-capcompute/aurora-capcompute/aurora"
	"github.com/aurora-capcompute/aurora-dispatchers/memory"
	"github.com/aurora-capcompute/aurora-dispatchers/registry"
	"github.com/aurora-capcompute/capcompute/sys"
)

// injectResolver is a test SecretResolver backed by a map.
type injectResolver map[string]string

func (m injectResolver) Resolve(name string) (string, bool) { v, ok := m[name]; return v, ok }

// End to end through the whole assembly — provider → registry → internet driver
// → the wire — a manifest's host-held credential reaches the request the guest
// asked for, the guest never supplies or sees it, and the journal-facing result
// records the credential's fingerprint, never its value. Loopback http is used
// only so the test can observe the header the host attached.
func TestProviderInjectsCredentialEndToEnd(t *testing.T) {
	var gotAuth string
	var gotHits int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHits++
		gotAuth = r.Header.Get("Authorization")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()

	provider := newProvider(
		[]registry.Registration{registry.InternetRegistration{}},
		registry.Services{
			Tenant:      "acme",
			MemoryStore: memory.NewMapStore(),
			Secrets:     injectResolver{"ONYX_TOKEN": "tok-abc"},
			AuditKey:    []byte("audit-key"),
		},
	)
	// The origin is the loopback test server (injection permits http only on
	// loopback); the SSRF guard is opened for it with allow_private_network.
	config := fmt.Sprintf(
		`{"allow_private_network":true,"capabilities":[{"methods":["GET"],"domain":%q,`+
			`"inject_headers":{"Authorization":{"secret":"ONYX_TOKEN","prefix":"Bearer "}}}]}`,
		server.URL)
	manifest := aurora.Manifest{
		Version: aurora.ManifestVersion,
		Syscalls: []aurora.Syscall{
			{Syscall: "core.internet", Config: json.RawMessage(config)},
		},
	}
	dispatcher, err := provider.NewDispatcher(context.Background(), aurora.ProcessContext{}, manifest)
	if err != nil {
		t.Fatalf("new dispatcher: %v", err)
	}

	// The guest asks only for method + url — it never names the credential.
	args, _ := json.Marshal(map[string]string{"method": "GET", "url": server.URL + "/v1/data"})
	result, err := dispatcher.Dispatch(context.Background(), aurora.ProcessContext{},
		sys.Syscall{Name: "core.internet", Args: args}, sys.Authorization{})
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if result.Status() != sys.StatusResult {
		t.Fatalf("result = %#v, want a successful result", result)
	}
	if gotHits != 1 {
		t.Fatalf("server hits = %d, want 1", gotHits)
	}
	if gotAuth != "Bearer tok-abc" {
		t.Fatalf("Authorization reaching the server = %q, want %q", gotAuth, "Bearer tok-abc")
	}

	// The result carries the credential's provenance for the journal — the keyed
	// fingerprint, never the token.
	want := "credential:ONYX_TOKEN@" + registry.CredentialFingerprint([]byte("audit-key"), "tok-abc")
	if !slices.Contains(result.Labels(), want) {
		t.Fatalf("result labels = %v, want the credential fingerprint %q", result.Labels(), want)
	}
	for _, label := range result.Labels() {
		if strings.Contains(label, "tok-abc") {
			t.Fatalf("SECURITY: the token value leaked into a result label: %q", label)
		}
	}
}

// A manifest that references a secret the host cannot resolve fails to build the
// dispatcher — at activation — rather than dispatching without the credential.
func TestProviderInjectionFailsClosedOnMissingSecret(t *testing.T) {
	provider := newProvider(
		[]registry.Registration{registry.InternetRegistration{}},
		registry.Services{Tenant: "acme", MemoryStore: memory.NewMapStore(), Secrets: injectResolver{}},
	)
	manifest := aurora.Manifest{
		Version: aurora.ManifestVersion,
		Syscalls: []aurora.Syscall{
			{Syscall: "core.internet", Config: json.RawMessage(
				`{"capabilities":[{"methods":["GET"],"domain":"https://onyx.example.com",` +
					`"inject_headers":{"Authorization":{"secret":"ONYX_TOKEN","prefix":"Bearer "}}}]}`)},
		},
	}
	if _, err := provider.NewDispatcher(context.Background(), aurora.ProcessContext{}, manifest); err == nil {
		t.Fatal("SECURITY: dispatcher built despite an unresolvable injected credential")
	}
}
