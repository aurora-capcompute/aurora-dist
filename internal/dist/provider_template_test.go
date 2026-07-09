package dist

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aurora-capcompute/aurora-capcompute/aurora"
	"github.com/aurora-capcompute/aurora-dispatchers/memory"
	"github.com/aurora-capcompute/aurora-dispatchers/registry"
	"github.com/aurora-capcompute/capcompute/sys"
)

// End to end through the distribution: a core.httpTemplate grant lets the guest
// invoke a named operation with only a {query} parameter; the host builds the
// exact request — fixed method, path, and body shape — attaches the host-held
// credential the guest never sees, and the guest cannot reach anything else on
// the origin. Loopback http is used only so the test can observe the request.
func TestProviderTemplateEndToEnd(t *testing.T) {
	var gotAuth, gotPath, gotBody string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotPath = r.URL.Path
		body, _ := io.ReadAll(r.Body)
		gotBody = string(body)
		_, _ = w.Write([]byte(`{"answer":"ok"}`))
	}))
	defer server.Close()

	provider := newProvider(
		[]registry.Registration{registry.HTTPTemplateRegistration{}},
		registry.Services{
			Tenant:      "acme",
			MemoryStore: memory.NewMapStore(),
			Secrets:     injectResolver{"ONYX_TOKEN": "tok-abc"},
			AuditKey:    []byte("audit-key"),
		},
	)
	config := fmt.Sprintf(`{"base_url":%q,"allow_private_network":true,`+
		`"operations":[{"name":"search","method":"POST","path":"/api/search",`+
		`"inject_headers":{"Authorization":{"secret":"ONYX_TOKEN","prefix":"Bearer "}},`+
		`"body":{"message":"{{query}}","persona_id":0},`+
		`"params":{"query":{"type":"string","required":true}}}]}`, server.URL)
	manifest := aurora.Manifest{
		Version: aurora.ManifestVersion,
		Syscalls: []aurora.Syscall{
			{Syscall: "core.httpTemplate", Config: json.RawMessage(config)},
		},
	}
	dispatcher, err := provider.NewDispatcher(context.Background(), aurora.ProcessContext{}, manifest)
	if err != nil {
		t.Fatalf("new dispatcher: %v", err)
	}

	// The guest supplies only the operation and its declared parameter.
	args, _ := json.Marshal(map[string]any{"operation": "search", "query": "What is Hwaas?"})
	result, err := dispatcher.Dispatch(context.Background(), aurora.ProcessContext{},
		sys.Syscall{Name: "core.httpTemplate", Args: args}, sys.Authorization{})
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if result.Status() != sys.StatusResult {
		t.Fatalf("result = %#v, want a successful result", result)
	}
	if gotPath != "/api/search" {
		t.Fatalf("server path = %q, want the fixed /api/search", gotPath)
	}
	if gotAuth != "Bearer tok-abc" {
		t.Fatalf("Authorization at the server = %q, want the injected credential", gotAuth)
	}
	if gotBody != `{"message":"What is Hwaas?","persona_id":0}` {
		t.Fatalf("body at the server = %q, want the templated body", gotBody)
	}
}

// A core.httpTemplate grant referencing a secret the host cannot resolve fails
// to build the dispatcher — at activation — rather than dispatching without it.
func TestProviderTemplateFailsClosedOnMissingSecret(t *testing.T) {
	provider := newProvider(
		[]registry.Registration{registry.HTTPTemplateRegistration{}},
		registry.Services{Tenant: "acme", MemoryStore: memory.NewMapStore(), Secrets: injectResolver{}},
	)
	manifest := aurora.Manifest{
		Version: aurora.ManifestVersion,
		Syscalls: []aurora.Syscall{
			{Syscall: "core.httpTemplate", Config: json.RawMessage(
				`{"base_url":"https://onyx.example.com",` +
					`"inject_headers":{"Authorization":{"secret":"ONYX_TOKEN","prefix":"Bearer "}},` +
					`"operations":[{"name":"search","method":"POST","path":"/api/search"}]}`)},
		},
	}
	if _, err := provider.NewDispatcher(context.Background(), aurora.ProcessContext{}, manifest); err == nil {
		t.Fatal("SECURITY: dispatcher built despite an unresolvable injected credential")
	}
}
