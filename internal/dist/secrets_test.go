package dist

import (
	"fmt"
	"testing"
)

func TestEnvSecretResolverResolves(t *testing.T) {
	env := map[string]string{
		"AURORA_SECRET_ONYX_TOKEN":      "tok-inline",
		"AURORA_SECRET_FROM_FILE_FILE":  "/secrets/from_file",
		"AURORA_SECRET_BOTH":            "inline-loses",
		"AURORA_SECRET_BOTH_FILE":       "/secrets/both",
		"AURORA_SECRET_BLANK":           "",
		"AURORA_SECRET_EMPTY_FILE_FILE": "/secrets/empty",
		// A direct env var carrying a trailing newline (from $(cat token), a .env
		// line, or a k8s/secrets-manager injection) must be trimmed like the file
		// form — otherwise the whitespace rides into e.g. an Authorization header.
		"AURORA_SECRET_PADDED": "  tok-padded\n",
		"AURORA_SECRET_WSONLY": "   \n\t",
	}
	files := map[string]string{
		"/secrets/from_file": "  tok-from-file\n",
		"/secrets/both":      "file-wins",
		"/secrets/empty":     "   ",
	}
	resolver := EnvSecretResolver{
		lookup: func(k string) (string, bool) { v, ok := env[k]; return v, ok },
		readFile: func(path string) ([]byte, error) {
			v, ok := files[path]
			if !ok {
				return nil, fmt.Errorf("no such file %q", path)
			}
			return []byte(v), nil
		},
	}

	cases := []struct {
		name      string
		ref       string
		wantValue string
		wantOK    bool
	}{
		{"inline value", "ONYX_TOKEN", "tok-inline", true},
		{"inline value is trimmed", "PADDED", "tok-padded", true},
		{"whitespace-only inline value", "WSONLY", "", false},
		{"file value is trimmed", "FROM_FILE", "tok-from-file", true},
		{"file wins over inline", "BOTH", "file-wins", true},
		{"unknown reference", "MISSING", "", false},
		{"blank inline value", "BLANK", "", false},
		{"empty file", "EMPTY_FILE", "", false},
		// A name that is not a plain env token can never map to AURORA_SECRET_*.
		{"path traversal", "../../etc/passwd", "", false},
		{"dotted name", "onyx.token", "", false},
		{"dashed name", "onyx-token", "", false},
		{"empty name", "", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			value, ok := resolver.Resolve(tc.ref)
			if ok != tc.wantOK || value != tc.wantValue {
				t.Fatalf("Resolve(%q) = (%q, %v), want (%q, %v)", tc.ref, value, ok, tc.wantValue, tc.wantOK)
			}
		})
	}
}

// A file the resolver cannot read is a miss, not a panic — the driver build then
// fails closed at activation with a clear "secret not available".
func TestEnvSecretResolverUnreadableFileIsMiss(t *testing.T) {
	resolver := EnvSecretResolver{
		lookup:   func(string) (string, bool) { return "/does/not/exist", true },
		readFile: func(string) ([]byte, error) { return nil, fmt.Errorf("boom") },
	}
	if v, ok := resolver.Resolve("ANY"); ok {
		t.Fatalf("Resolve returned %q for an unreadable secret file", v)
	}
}

// The real constructor reads the actual process environment.
func TestNewEnvSecretResolverReadsProcessEnv(t *testing.T) {
	t.Setenv("AURORA_SECRET_ONYX_TOKEN", "tok-live")
	if v, ok := NewEnvSecretResolver().Resolve("ONYX_TOKEN"); !ok || v != "tok-live" {
		t.Fatalf("Resolve = (%q, %v), want (tok-live, true)", v, ok)
	}
	if _, ok := NewEnvSecretResolver().Resolve("NOT_SET_ANYWHERE"); ok {
		t.Fatal("resolved a reference with no environment variable set")
	}
}
