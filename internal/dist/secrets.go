package dist

import (
	"os"
	"regexp"
	"strings"
)

// secretNamePattern constrains a manifest secret reference to a safe environment
// identifier, so a reference can never escape the AURORA_SECRET_ namespace — no
// path separators, no dots, nothing but a plain env-var token.
var secretNamePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// EnvSecretResolver resolves manifest secret references from the host process
// environment. A reference NAME (e.g. ONYX_TOKEN) resolves from the environment
// variable AURORA_SECRET_NAME, or — preferred — from the file whose path is in
// AURORA_SECRET_NAME_FILE. Reading from a file keeps the value out of the
// process environment block (which is world-readable via /proc/<pid>/environ)
// and lets a secret manager mount it. Only the reference name ever appears in a
// manifest; the value is host-held and never enters the manifest, the journal,
// or the guest. An unknown or malformed reference resolves to not-found, so the
// driver build fails closed rather than dispatching without the credential.
type EnvSecretResolver struct {
	// lookup and readFile are the environment and filesystem seams (os.LookupEnv,
	// os.ReadFile), overridable in tests.
	lookup   func(string) (string, bool)
	readFile func(string) ([]byte, error)
}

// NewEnvSecretResolver returns a resolver backed by the real process
// environment and filesystem.
func NewEnvSecretResolver() EnvSecretResolver {
	return EnvSecretResolver{lookup: os.LookupEnv, readFile: os.ReadFile}
}

// Resolve looks up a reference name. The _FILE form wins when both are set, so a
// mounted secret file is never shadowed by a stale inline variable.
func (r EnvSecretResolver) Resolve(name string) (string, bool) {
	if !secretNamePattern.MatchString(name) {
		return "", false
	}
	if path, ok := r.lookup("AURORA_SECRET_" + name + "_FILE"); ok {
		raw, err := r.readFile(strings.TrimSpace(path))
		if err != nil {
			return "", false
		}
		value := strings.TrimSpace(string(raw))
		return value, value != ""
	}
	if value, ok := r.lookup("AURORA_SECRET_" + name); ok && value != "" {
		return value, true
	}
	return "", false
}
