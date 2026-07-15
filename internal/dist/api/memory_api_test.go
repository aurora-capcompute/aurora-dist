package api_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aurora-capcompute/aurora-dist/internal/dist"
	"github.com/aurora-capcompute/aurora-dist/internal/dist/api"
)

// The read-only memory endpoints' wire contract: list returns a keys array
// (empty store = empty list, not an error), a value read of an absent key is
// found:false, a value read without a key is a 400 invalid_args, and there is
// no write route (the only path into memory is the journaled syscall). The
// data path itself — keys, versions, labels round-tripping the store — is
// covered in the dist package, which can seed the store directly.
func TestMemoryEndpointsWireContract(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d, err := dist.New(ctx, dist.Config{
		ProgramsDir: t.TempDir(),
		TaskSecret:  []byte("wire-secret"),
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("assemble distribution: %v", err)
	}
	defer func() { _ = d.Close(context.Background()) }()
	server := httptest.NewServer(api.Handler(d))
	defer server.Close()

	get := func(path string) (int, []byte) {
		t.Helper()
		resp, err := server.Client().Get(server.URL + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, body
	}

	status, body := get("/v1/memory")
	var listed struct {
		Keys []dist.MemoryEntry `json:"keys"`
	}
	if status != http.StatusOK {
		t.Fatalf("list = %d %s", status, body)
	}
	if err := json.Unmarshal(body, &listed); err != nil || len(listed.Keys) != 0 {
		t.Fatalf("empty list = %s (%v), want keys:[]", body, err)
	}

	status, body = get("/v1/memory/value?key=shared/absent")
	var value dist.MemoryValue
	if status != http.StatusOK || json.Unmarshal(body, &value) != nil || value.Found {
		t.Fatalf("absent value = %d %s", status, body)
	}

	if status, body = get("/v1/memory/value"); status != http.StatusBadRequest {
		t.Fatalf("missing key param = %d %s, want 400", status, body)
	}

	// No write route: PUT/POST/DELETE on the memory paths are method-not-allowed.
	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete} {
		req, _ := http.NewRequest(method, server.URL+"/v1/memory/value?key=x", nil)
		resp, err := server.Client().Do(req)
		if err != nil {
			t.Fatalf("%s: %v", method, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Fatalf("%s memory = %d, want 405 (memory is read-only from the API)", method, resp.StatusCode)
		}
	}
}
