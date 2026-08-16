package dist

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aurora-capcompute/aurora-capcompute/capability"
	"github.com/aurora-capcompute/aurora-capcompute/monitor"
	"github.com/aurora-capcompute/aurora-dispatchers/registry"
	"github.com/aurora-capcompute/capcompute/sys"
)

// The menu a guest is handed is projected from the entries the table routes, and
// the reference monitor admits every call against it. So the projection has to
// survive the case where several operations share one argument shape: core.internet
// tells GET from POST by a `method` its schema does not pin, and a union over two
// copies of that one shape matches every instance twice — which is one too many.
// Such a menu admits nothing at all, and a plainly granted GET would be refused
// by the Validator before routing ever saw it.
//
// The counterpart matters as much: an ungranted method is still denied, by the
// index rather than by the schema. That is why the flat shape is the right menu
// and not a hole — the grant is the table, never the advertisement.
func TestInternetMenuAdmitsAGrantedCall(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`ok`))
	}))
	defer origin.Close()

	table := capability.NewTable()
	config := fmt.Sprintf(`{"allow_private_network":true,"capabilities":[{"methods":["GET","POST"],"domain":%q}]}`, origin.URL)
	family, err := (registry.InternetRegistration{}).Configure(
		context.Background(), json.RawMessage(config), registry.Services{})
	if err != nil {
		t.Fatalf("configure internet: %v", err)
	}
	if err := table.Add(family); err != nil {
		t.Fatalf("add: %v", err)
	}

	chain := monitor.NewValidator[flowCred](table, capability.NewDispatcher[flowCred](table))
	call := func(method string) sys.SyscallResult {
		raw, _ := json.Marshal(map[string]any{"method": method, "url": origin.URL + "/thing"})
		result, err := chain.Dispatch(context.Background(), flowCred{"menu"},
			sys.Syscall{Abi: sys.ABIVersion, Name: "core.internet", Args: raw}, sys.Authorization{})
		if err != nil {
			t.Fatalf("dispatch %s: %v", method, err)
		}
		return result
	}

	for _, method := range []string{"GET", "POST"} {
		if result := call(method); result.Status() != sys.StatusResult {
			t.Fatalf("granted %s = %v/%v, want a result — the published menu rejects a call the table routes",
				method, result.Status(), result.Errno())
		}
	}
	if denied := call("PUT"); denied.Errno() != sys.ErrnoDenied {
		t.Fatalf("ungranted PUT = %v/%v, want denied by the index", denied.Status(), denied.Errno())
	}
}
