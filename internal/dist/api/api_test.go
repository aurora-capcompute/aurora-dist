package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// The Host guard must admit any IP literal (which cannot be DNS-rebound) and
// localhost, and refuse a domain name (the rebinding vector), so a loopback
// client keeps working while a browser lured to attacker.example cannot reach
// the unauthenticated API by rebinding it to 127.0.0.1.
func TestAllowedHostDefeatsRebinding(t *testing.T) {
	cases := map[string]bool{
		"127.0.0.1:8080":        true,
		"127.0.0.1":             true,
		"localhost:8080":        true,
		"localhost":             true,
		"[::1]:8080":            true,
		"192.168.1.5:8080":      true, // an IP literal cannot be rebound
		"attacker.example:8080": false,
		"attacker.example":      false,
		"":                      true, // HTTP/1.0 or a proxy stripped it
	}
	for host, want := range cases {
		if got := allowedHost(host); got != want {
			t.Fatalf("allowedHost(%q) = %v, want %v", host, got, want)
		}
	}
}

func TestGuardRebindingRejectsDomainHost(t *testing.T) {
	guarded := guardRebinding(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	rebind := httptest.NewRequest(http.MethodPost, "http://x/v1/sessions", nil)
	rebind.Host = "attacker.example"
	rec := httptest.NewRecorder()
	guarded.ServeHTTP(rec, rebind)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("domain-Host request = %d, want 403 (rebinding refused)", rec.Code)
	}

	loopback := httptest.NewRequest(http.MethodPost, "http://x/v1/sessions", nil)
	loopback.Host = "127.0.0.1:8080"
	rec = httptest.NewRecorder()
	guarded.ServeHTTP(rec, loopback)
	if rec.Code != http.StatusOK {
		t.Fatalf("loopback request = %d, want 200", rec.Code)
	}
}
