package api_test

// The distribution end-to-end: the real Rust agent program (built from the
// sibling aurora-brains checkout) driven through the real HTTP API, with an
// OpenAI-compatible stub as cognition. The scripted model first sets a
// one-second timer — exercising the durable-task path and the runtime's task
// sweep, which fires it — then finishes. Everything is observed the way
// a terminal would observe it: REST snapshots and the journal.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aurora-capcompute/aurora-capcompute/aurora"

	"github.com/aurora-capcompute/aurora-dist/internal/dist"
	"github.com/aurora-capcompute/aurora-dist/internal/dist/api"
)

var (
	programOnce  sync.Once
	programWasm  []byte
	programError error
)

// buildProgram compiles the Rust agent program from the sibling aurora-brains
// workspace to wasm32-wasip1 — the same artifact a real deployment ships.
func buildProgram(t *testing.T) []byte {
	t.Helper()
	if _, err := exec.LookPath("cargo"); err != nil {
		t.Skip("cargo not found")
	}
	programOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		cmd := exec.CommandContext(ctx, "cargo", "build",
			"--release",
			"--target", "wasm32-wasip1",
			"-p", "agent",
		)
		cmd.Dir = "../../../../aurora-brains"
		if out, err := cmd.CombinedOutput(); err != nil {
			programError = fmt.Errorf("build program: %v\n%s", err, out)
			return
		}
		wasmPath := filepath.Join(cmd.Dir, "target", "wasm32-wasip1", "release", "agent.wasm")
		raw, err := os.ReadFile(wasmPath)
		if err != nil {
			programError = fmt.Errorf("read program: %v", err)
			return
		}
		programWasm = raw
	})
	if programError != nil {
		t.Skipf("agent program unavailable: %v", programError)
	}
	return programWasm
}

// writeProgramDir drops the pair a programs directory loads: agent.wasm and its
// agent.json interface manifest.
func writeProgramDir(t *testing.T, dir string, wasm []byte) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, "agent.wasm"), wasm, 0o600); err != nil {
		t.Fatal(err)
	}
	iface := `{"description":"the agent","input":{"type":"string"},"output":{"type":"string"}}`
	if err := os.WriteFile(filepath.Join(dir, "agent.json"), []byte(iface), 0o600); err != nil {
		t.Fatal(err)
	}
}

// scriptedLLM is an OpenAI-compatible chat stub: until it has seen a timer
// observation it asks for sys.timer; afterwards it finishes.
func scriptedLLM(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/chat/completions") {
			http.NotFound(w, r)
			return
		}
		body, _ := io.ReadAll(r.Body)
		reply := `{"actions":[{"action":"sys.timer","content":{"duration_seconds":1,"label":"nap"}}]}`
		if bytes.Contains(body, []byte(`fired`)) {
			reply = `{"actions":[{"action":"final","content":{"answer":"woke up after the nap"}}]}`
		}
		payload, _ := json.Marshal(map[string]any{
			"choices": []any{map[string]any{"message": map[string]any{"content": reply}}},
		})
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(payload)
	}))
}

// e2eSecrets holds the stub LLM's key: manifests name it, the host resolves it.
var e2eSecrets = secretMap{"OPENAI_KEY": "test-key"}

type secretMap map[string]string

func (m secretMap) Resolve(name string) (string, bool) { v, ok := m[name]; return v, ok }

func testManifest(llmBaseURL string) aurora.Manifest {
	config, _ := json.Marshal(map[string]any{
		"base_url":            llmBaseURL,
		"api_key":             map[string]string{"secret": "OPENAI_KEY"},
		"allow_insecure_http": true,
		"default_model":       "stub-model",
		"capabilities": []map[string]any{
			{"operation": "chat", "require_approval": false},
		},
	})
	// No sys.timer grant: the nap tests run on the default timer.
	return aurora.Manifest{
		Version: aurora.ManifestVersion,
		Program: "agent",
		Syscalls: []aurora.Syscall{
			{Syscall: "core.openaiApi", Config: config, Hidden: true},
		},
		Skills: []aurora.Skill{{
			Name:         "duty",
			Description:  "answer duty requests",
			Instructions: "Name the service you checked in every answer.",
		}},
	}
}

type client struct {
	t    *testing.T
	base string
	http *http.Client
}

func (c *client) do(method, path string, body any, out any) *http.Response {
	c.t.Helper()
	var reader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			c.t.Fatal(err)
		}
		reader = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, c.base+path, reader)
	if err != nil {
		c.t.Fatal(err)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		c.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		c.t.Fatalf("%s %s: %d %s", method, path, resp.StatusCode, raw)
	}
	if out != nil {
		if err := json.Unmarshal(raw, out); err != nil {
			c.t.Fatalf("%s %s: decode %v (%s)", method, path, err, raw)
		}
	}
	return resp
}

func TestDistributionEndToEnd(t *testing.T) {
	wasm := buildProgram(t)

	programsDir := t.TempDir()
	writeProgramDir(t, programsDir, wasm)
	llm := scriptedLLM(t)
	defer llm.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d, err := dist.New(ctx, dist.Config{
		Manifest:    testManifest(llm.URL + "/v1"),
		Secrets:     e2eSecrets,
		DataDir:     "", // in-memory
		ProgramsDir: programsDir,
		TaskSecret:  []byte("e2e-secret"),
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("assemble distribution: %v", err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer closeCancel()
		_ = d.Close(closeCtx)
	}()

	server := httptest.NewServer(api.Handler(d, nil))
	defer server.Close()
	c := &client{t: t, base: server.URL, http: server.Client()}

	// The loaded program set is readable — the terminal's `ls /programs` — and
	// each entry carries the interface the program declares (description +
	// input/output schemas), read from the sidecar manifest beside the wasm at
	// load. The response shape is the API's own: no wasm bytes reach a client.
	var programs []struct {
		ID          string          `json:"id"`
		Digest      string          `json:"digest"`
		Description string          `json:"description"`
		Input       json.RawMessage `json:"input"`
		Output      json.RawMessage `json:"output"`
	}
	c.do(http.MethodGet, "/v1/programs", nil, &programs)
	if len(programs) != 1 || programs[0].ID != "agent" || programs[0].Digest == "" {
		t.Fatalf("programs = %+v, want the loaded agent program with its digest", programs)
	}
	if programs[0].Description == "" || len(programs[0].Input) == 0 || len(programs[0].Output) == 0 {
		t.Fatalf("program interface not surfaced: %+v", programs[0])
	}

	// Create a session, start a process.
	var session aurora.SessionSnapshot
	c.do(http.MethodPost, "/v1/sessions", map[string]any{"tags": map[string]string{"origin": "e2e"}}, &session)
	if !strings.HasPrefix(session.ID, "ses_") {
		t.Fatalf("session id = %q", session.ID)
	}
	sessionID := session.ID

	refused, err := server.Client().Post(server.URL+"/v1/sessions/"+sessionID+"/processes", "application/json",
		strings.NewReader(`{"input":"hi","manifest":{"version":4,"program":"agent","syscalls":[{"syscall":"sys.timer"}]}}`))
	if err != nil {
		t.Fatal(err)
	}
	refused.Body.Close()
	if refused.StatusCode != http.StatusBadRequest {
		t.Fatalf("create process with a client manifest = %d, want 400", refused.StatusCode)
	}

	var process aurora.ProcessSnapshot
	c.do(http.MethodPost, "/v1/sessions/"+sessionID+"/processes", map[string]any{
		"input": "take a nap, then report back",
	}, &process)
	if !strings.HasPrefix(process.ID, "proc_") || process.SessionID != sessionID {
		t.Fatalf("process = %+v", process)
	}

	// The process sets a 1s timer (a durable task) which the runtime's task
	// sweep fires; then the model finishes.
	deadline := time.Now().Add(60 * time.Second)
	for {
		c.do(http.MethodGet, "/v1/processes/"+process.ID, nil, &process)
		if process.Status == aurora.ProcessCompleted {
			break
		}
		if process.Status == aurora.ProcessFailed || process.Status == aurora.ProcessStopped {
			t.Fatalf("process finished as %s: %s", process.Status, process.Error)
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out in status %s", process.Status)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if process.Answer != "woke up after the nap" {
		t.Fatalf("answer = %q", process.Answer)
	}

	// The one session read carries everything: the folded conversation, and
	// each process's full journal and tasks — no separate journal/tasks/graph
	// endpoints. The journal narrates the whole story: input → chat → timer →
	// chat → finish.
	c.do(http.MethodGet, "/v1/sessions/"+sessionID, nil, &session)
	if len(session.Processes) != 1 || session.ActiveProcessID != "" || len(session.History) != 2 {
		t.Fatalf("session = %+v", session)
	}
	logged := session.Processes[0]
	var names []string
	for _, entry := range logged.Entries {
		names = append(names, entry.Syscall.Name)
	}
	story := strings.Join(names, " ")
	for _, want := range []string{"sys.input", "core.openaiApi", "sys.timer", "sys.output"} {
		if !strings.Contains(story, want) {
			t.Fatalf("journal %v is missing %s", names, want)
		}
	}

	// A journaled result crosses the wire as the JSON document it is — the
	// terminal renders it — not as an encoded blob.
	var resulted int
	for _, entry := range logged.Entries {
		if entry.Outcome.Status != "result" || len(entry.Outcome.Result) == 0 {
			continue
		}
		resulted++
		if !json.Valid(entry.Outcome.Result) {
			t.Fatalf("%s result is not JSON on the wire: %s", entry.Syscall.Name, entry.Outcome.Result)
		}
	}
	if resulted == 0 {
		t.Fatal("no journal entry carried a result; the round-trip is untested")
	}

	// The timer task resolved as completed by the timer actor.
	tasks := logged.Tasks
	if len(tasks) != 1 || tasks[0].State != aurora.TaskStateCompleted {
		t.Fatalf("tasks = %+v", tasks)
	}
	if tasks[0].Resolution.Actor != "timer" {
		t.Fatalf("timer task resolved by %q", tasks[0].Resolution.Actor)
	}
}

// A distribution restart mid-wait: the process parks on a durable timer task
// in SQLite, the whole distribution shuts down, and a fresh assembly restores
// the session, fires the elapsed timer, and drives the process to completion —
// drain-and-restart with no live state carried over.
func TestDistributionRestartRecoversTimers(t *testing.T) {
	wasm := buildProgram(t)

	programsDir := t.TempDir()
	writeProgramDir(t, programsDir, wasm)
	dataDir := t.TempDir()
	llm := scriptedLLM(t)
	defer llm.Close()

	config := dist.Config{
		Manifest:    testManifest(llm.URL + "/v1"),
		Secrets:     e2eSecrets,
		DataDir:     dataDir,
		ProgramsDir: programsDir,
		TaskSecret:  []byte("e2e-secret"),
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	first, err := dist.New(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(api.Handler(first, nil))
	c := &client{t: t, base: server.URL, http: server.Client()}

	var session aurora.SessionSnapshot
	c.do(http.MethodPost, "/v1/sessions", nil, &session)
	sessionID := session.ID
	var process aurora.ProcessSnapshot
	c.do(http.MethodPost, "/v1/sessions/"+sessionID+"/processes", map[string]any{
		"input": "take a nap, then report back",
	}, &process)

	// Wait for the park on the timer task, then shut the first instance down
	// before the one-second timer can fire.
	deadline := time.Now().Add(30 * time.Second)
	for process.Status != aurora.ProcessWaitingTask {
		if time.Now().After(deadline) {
			t.Fatalf("never parked; status %s", process.Status)
		}
		time.Sleep(20 * time.Millisecond)
		c.do(http.MethodGet, "/v1/processes/"+process.ID, nil, &process)
	}
	server.Close()
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 15*time.Second)
	if err := first.Close(closeCtx); err != nil {
		t.Fatalf("close first instance: %v", err)
	}
	closeCancel()

	// Let the fire time elapse while nothing is running.
	time.Sleep(1100 * time.Millisecond)

	second, err := dist.New(context.Background(), config)
	if err != nil {
		t.Fatalf("reassemble: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = second.Close(ctx)
	}()
	server = httptest.NewServer(api.Handler(second, nil))
	defer server.Close()
	c = &client{t: t, base: server.URL, http: server.Client()}

	// The new instance's first sweep fired the elapsed timer, and the process
	// finished on it.
	deadline = time.Now().Add(60 * time.Second)
	for {
		c.do(http.MethodGet, "/v1/processes/"+process.ID, nil, &process)
		if process.Status == aurora.ProcessCompleted {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out in status %s (%s)", process.Status, process.Error)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if process.Answer != "woke up after the nap" {
		t.Fatalf("answer = %q", process.Answer)
	}
}

// A process interrupted mid-run by a host failure is resumed automatically on
// restart — no human, no manual retry. The scripted model blocks on its first
// call; the distribution is torn down while the process is running (mid-call),
// leaving it interrupted (restore folds a running process to interrupted); a
// fresh instance over the same store re-drives it to completion.
func TestDistributionResumesInterruptedProcess(t *testing.T) {
	wasm := buildProgram(t)

	programsDir := t.TempDir()
	writeProgramDir(t, programsDir, wasm)
	dataDir := t.TempDir()

	// The model blocks its first call (so the process is caught mid-run) and,
	// once re-driven on the new instance, finishes.
	release := make(chan struct{})
	var calls int32
	llm := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/chat/completions") {
			http.NotFound(w, r)
			return
		}
		if atomic.AddInt32(&calls, 1) == 1 {
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
		}
		reply := `{"actions":[{"action":"final","content":{"answer":"resumed after crash"}}]}`
		payload, _ := json.Marshal(map[string]any{
			"choices": []any{map[string]any{"message": map[string]any{"content": reply}}},
		})
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(payload)
	}))
	defer llm.Close()

	config := dist.Config{
		Manifest:    testManifest(llm.URL + "/v1"),
		Secrets:     e2eSecrets,
		DataDir:     dataDir,
		ProgramsDir: programsDir,
		TaskSecret:  []byte("e2e-secret"),
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	first, err := dist.New(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(api.Handler(first, nil))
	c := &client{t: t, base: server.URL, http: server.Client()}

	var session aurora.SessionSnapshot
	c.do(http.MethodPost, "/v1/sessions", nil, &session)
	var process aurora.ProcessSnapshot
	c.do(http.MethodPost, "/v1/sessions/"+session.ID+"/processes", map[string]any{
		"input": "do the thing",
	}, &process)

	// Catch the process actively running (blocked in the model call).
	deadline := time.Now().Add(30 * time.Second)
	for process.Status != aurora.ProcessRunning {
		if time.Now().After(deadline) {
			t.Fatalf("process never started running; status %s", process.Status)
		}
		time.Sleep(20 * time.Millisecond)
		c.do(http.MethodGet, "/v1/processes/"+process.ID, nil, &process)
	}

	// Host failure: tear the instance down mid-run, then release the abandoned call.
	server.Close()
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 15*time.Second)
	_ = first.Close(closeCtx)
	closeCancel()
	close(release)

	// A fresh instance over the same store resumes the interrupted process with
	// no intervention.
	second, err := dist.New(context.Background(), config)
	if err != nil {
		t.Fatalf("reassemble: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = second.Close(ctx)
	}()
	server = httptest.NewServer(api.Handler(second, nil))
	defer server.Close()
	c = &client{t: t, base: server.URL, http: server.Client()}

	deadline = time.Now().Add(60 * time.Second)
	for {
		c.do(http.MethodGet, "/v1/processes/"+process.ID, nil, &process)
		if process.Status == aurora.ProcessCompleted {
			break
		}
		if process.Status == aurora.ProcessFailed {
			t.Fatalf("process failed after restart instead of resuming: %s", process.Error)
		}
		if time.Now().After(deadline) {
			t.Fatalf("interrupted process was not resumed; status %s", process.Status)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if process.Answer != "resumed after crash" {
		t.Fatalf("answer = %q", process.Answer)
	}
}

// TestAgentLoopIsCapped proves the reasoning loop is bounded: a model that never
// finishes — it asks for a synchronous tool (a memory read) on every turn — is
// forced to a final answer at the guest's step cap, so the process completes
// instead of looping forever. The stub counts its calls: 15 tool turns plus the
// one forced-final turn is exactly the 16-step cap, and never more.
func TestAgentLoopIsCapped(t *testing.T) {
	wasm := buildProgram(t)
	programsDir := t.TempDir()
	writeProgramDir(t, programsDir, wasm)

	var calls atomic.Int64
	llm := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/chat/completions") {
			http.NotFound(w, r)
			return
		}
		calls.Add(1)
		// Never finish: always ask for a synchronous memory read so the loop
		// would run forever if it were not capped.
		reply := `{"actions":[{"action":"core.scratch","content":{"operation":"get","key":"k"}}]}`
		payload, _ := json.Marshal(map[string]any{
			"choices": []any{map[string]any{"message": map[string]any{"content": reply}}},
		})
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(payload)
	}))
	defer llm.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	llmConfig, _ := json.Marshal(map[string]any{
		"base_url":            llm.URL + "/v1",
		"api_key":             map[string]string{"secret": "OPENAI_KEY"},
		"allow_insecure_http": true,
		"default_model":       "stub-model",
		"capabilities":        []map[string]any{{"operation": "chat", "require_approval": false}},
	})
	memConfig, _ := json.Marshal(map[string]any{
		"capabilities": []map[string]any{{"operation": "get"}},
	})
	manifest := aurora.Manifest{
		Version: aurora.ManifestVersion,
		Program: "agent",
		Syscalls: []aurora.Syscall{
			{Syscall: "core.openaiApi", Config: llmConfig, Hidden: true},
			{Syscall: "core.scratch", Config: memConfig},
		},
	}

	d, err := dist.New(ctx, dist.Config{
		Manifest:    manifest,
		Secrets:     e2eSecrets,
		ProgramsDir: programsDir,
		TaskSecret:  []byte("e2e-secret"),
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("assemble distribution: %v", err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer closeCancel()
		_ = d.Close(closeCtx)
	}()

	server := httptest.NewServer(api.Handler(d, nil))
	defer server.Close()
	c := &client{t: t, base: server.URL, http: server.Client()}

	var session aurora.SessionSnapshot
	c.do(http.MethodPost, "/v1/sessions", map[string]any{}, &session)
	var process aurora.ProcessSnapshot
	c.do(http.MethodPost, "/v1/sessions/"+session.ID+"/processes", map[string]any{
		"input": "keep going forever",
	}, &process)

	deadline := time.Now().Add(60 * time.Second)
	for {
		c.do(http.MethodGet, "/v1/processes/"+process.ID, nil, &process)
		if process.Status == aurora.ProcessCompleted {
			break
		}
		if process.Status == aurora.ProcessFailed || process.Status == aurora.ProcessStopped {
			t.Fatalf("process finished as %s: %s", process.Status, process.Error)
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out in %s after %d model calls — loop not capped", process.Status, calls.Load())
		}
		time.Sleep(50 * time.Millisecond)
	}

	if got := calls.Load(); got != 16 {
		t.Fatalf("model called %d times, want exactly the 16-step cap", got)
	}
}

// TestAgentSalvagesProseReply proves the guest does not fail when the model
// answers directly in prose instead of the JSON action envelope (a common
// break from smaller or non-OpenAI models): the reply is salvaged as the final
// answer, so the process completes carrying that prose rather than dying with
// "invalid model JSON".
func TestAgentSalvagesProseReply(t *testing.T) {
	wasm := buildProgram(t)
	programsDir := t.TempDir()
	writeProgramDir(t, programsDir, wasm)

	const prose = "Warp is a modern terminal with AI features and block-based output."
	llm := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/chat/completions") {
			http.NotFound(w, r)
			return
		}
		// Reply with bare prose — no {"actions":[...]} envelope at all.
		payload, _ := json.Marshal(map[string]any{
			"choices": []any{map[string]any{"message": map[string]any{"content": prose}}},
		})
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(payload)
	}))
	defer llm.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	llmConfig, _ := json.Marshal(map[string]any{
		"base_url":            llm.URL + "/v1",
		"api_key":             map[string]string{"secret": "OPENAI_KEY"},
		"allow_insecure_http": true,
		"default_model":       "stub-model",
		"capabilities":        []map[string]any{{"operation": "chat", "require_approval": false}},
	})
	manifest := aurora.Manifest{
		Version: aurora.ManifestVersion,
		Program: "agent",
		Syscalls: []aurora.Syscall{
			{Syscall: "core.openaiApi", Config: llmConfig, Hidden: true},
		},
	}

	d, err := dist.New(ctx, dist.Config{
		Manifest:    manifest,
		Secrets:     e2eSecrets,
		ProgramsDir: programsDir,
		TaskSecret:  []byte("e2e-secret"),
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("assemble distribution: %v", err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer closeCancel()
		_ = d.Close(closeCtx)
	}()

	server := httptest.NewServer(api.Handler(d, nil))
	defer server.Close()
	c := &client{t: t, base: server.URL, http: server.Client()}

	var session aurora.SessionSnapshot
	c.do(http.MethodPost, "/v1/sessions", map[string]any{}, &session)
	var process aurora.ProcessSnapshot
	c.do(http.MethodPost, "/v1/sessions/"+session.ID+"/processes", map[string]any{
		"input": "is warp a good terminal emulator?",
	}, &process)

	deadline := time.Now().Add(60 * time.Second)
	for {
		c.do(http.MethodGet, "/v1/processes/"+process.ID, nil, &process)
		if process.Status == aurora.ProcessCompleted {
			break
		}
		if process.Status == aurora.ProcessFailed || process.Status == aurora.ProcessStopped {
			t.Fatalf("process finished as %s: %s", process.Status, process.Error)
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out in %s", process.Status)
		}
		time.Sleep(50 * time.Millisecond)
	}

	if process.Answer != prose {
		t.Fatalf("answer = %q, want the salvaged prose %q", process.Answer, prose)
	}
}

// A large tool result is saved, and the model sees its key and its head, never
// the whole result. When the model asks to summarize it, the agent summarizes
// the saved value in a separate chat, and only the summary reaches the model.
// A marker at the end of the page, past the head, shows which chat saw what.
func TestAgentSavesLargeResultAndSummarizesItApart(t *testing.T) {
	wasm := buildProgram(t)
	programsDir := t.TempDir()
	writeProgramDir(t, programsDir, wasm)

	const deepMarker = "DEEP_BODY_SENTINEL_should_never_reach_the_model"
	// ~110 KB, past the 48 KB inline limit; the marker sits at the very end,
	// well beyond the 2 KB head excerpt.
	bigBody := strings.Repeat("lorem ipsum dolor sit amet consectetur adipiscing elit. ", 2000) + deepMarker

	internet := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = io.WriteString(w, bigBody)
	}))
	defer internet.Close()

	const summary = "The page is lorem ipsum with a sentinel at its end."
	savedKey := regexp.MustCompile(`io/[0-9a-f]{16}`)
	var turn atomic.Int32
	var sawKey, summarizerSawMarker, sawSummary, leaked atomic.Bool
	llm := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/chat/completions") {
			http.NotFound(w, r)
			return
		}
		body, _ := io.ReadAll(r.Body)
		var reply string
		if bytes.Contains(body, []byte("You summarize a saved tool result")) {
			// The separate summarize chat: it reads the whole saved value.
			summarizerSawMarker.Store(bytes.Contains(body, []byte(deepMarker)))
			reply = summary
		} else {
			if bytes.Contains(body, []byte(deepMarker)) {
				leaked.Store(true)
			}
			switch turn.Add(1) {
			case 1:
				reply = fmt.Sprintf(`{"actions":[{"action":"core.internet","content":{"method":"GET","url":%q}}]}`, internet.URL+"/page")
			case 2:
				key := savedKey.Find(body)
				sawKey.Store(key != nil)
				reply = fmt.Sprintf(`{"actions":[{"action":"summarize","content":{"key":%q,"focus":"what is on the page"}}]}`, key)
			default:
				sawSummary.Store(bytes.Contains(body, []byte(summary)))
				reply = `{"actions":[{"action":"final","content":{"answer":"done"}}]}`
			}
		}
		payload, _ := json.Marshal(map[string]any{
			"choices": []any{map[string]any{"message": map[string]any{"content": reply}}},
		})
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(payload)
	}))
	defer llm.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	llmConfig, _ := json.Marshal(map[string]any{
		"base_url":            llm.URL + "/v1",
		"api_key":             map[string]string{"secret": "OPENAI_KEY"},
		"allow_insecure_http": true,
		"default_model":       "stub-model",
		"capabilities":        []map[string]any{{"operation": "chat", "require_approval": false}},
	})
	internetConfig, _ := json.Marshal(map[string]any{
		"capabilities": []map[string]any{{"methods": []string{"GET"}, "domain": internet.URL}},
		// The test target is a loopback httptest server, which the SSRF guard
		// blocks by default; opt in as a real deployment reaching an internal
		// service would.
		"allow_private_network": true,
		// The whole page, marker included, must be saved.
		"max_response_bytes": 1 << 20,
	})
	// Results are saved in core.scratch, the session's store, and summarize
	// reads them back with get.
	scratchConfig, _ := json.Marshal(map[string]any{
		"capabilities": []map[string]any{{"operation": "store"}, {"operation": "get"}, {"operation": "search"}},
	})
	manifest := aurora.Manifest{
		Version: aurora.ManifestVersion,
		Program: "agent",
		Syscalls: []aurora.Syscall{
			{Syscall: "core.openaiApi", Config: llmConfig, Hidden: true},
			{Syscall: "core.internet", Config: internetConfig},
			{Syscall: "core.scratch", Config: scratchConfig},
		},
	}

	d, err := dist.New(ctx, dist.Config{
		Manifest:    manifest,
		Secrets:     e2eSecrets,
		ProgramsDir: programsDir,
		TaskSecret:  []byte("e2e-secret"),
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("assemble distribution: %v", err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer closeCancel()
		_ = d.Close(closeCtx)
	}()

	server := httptest.NewServer(api.Handler(d, nil))
	defer server.Close()
	c := &client{t: t, base: server.URL, http: server.Client()}

	var session aurora.SessionSnapshot
	c.do(http.MethodPost, "/v1/sessions", map[string]any{}, &session)
	var process aurora.ProcessSnapshot
	c.do(http.MethodPost, "/v1/sessions/"+session.ID+"/processes", map[string]any{
		"input": "research the page",
	}, &process)

	deadline := time.Now().Add(60 * time.Second)
	for {
		c.do(http.MethodGet, "/v1/processes/"+process.ID, nil, &process)
		if process.Status == aurora.ProcessCompleted {
			break
		}
		if process.Status == aurora.ProcessFailed || process.Status == aurora.ProcessStopped {
			t.Fatalf("process finished as %s: %s", process.Status, process.Error)
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out in %s", process.Status)
		}
		time.Sleep(50 * time.Millisecond)
	}

	if !sawKey.Load() {
		t.Fatal("the model never saw the saved result's key")
	}
	if !summarizerSawMarker.Load() {
		t.Fatal("the summarize chat did not read the whole saved result")
	}
	if !sawSummary.Load() {
		t.Fatal("the summary did not reach the model")
	}
	if leaked.Load() {
		t.Fatal("the whole result reached the model's conversation")
	}
}

// A saved result must still be there after the process parks and resumes. The
// page is saved in one quantum, a timer ends that quantum, and the search for
// its tail runs in the next one — where replay serves the store from the
// journal instead of running it again.
func TestScratchSurvivesAQuantumBoundary(t *testing.T) {
	wasm := buildProgram(t)
	programsDir := t.TempDir()
	writeProgramDir(t, programsDir, wasm)

	const deepMarker = "DEEP_BODY_SENTINEL_found_by_search"
	// The marker gets its own line: search returns matching lines capped in
	// length, so a marker at the end of one huge line would be cut off.
	bigBody := strings.Repeat("lorem ipsum dolor sit amet consectetur adipiscing elit.\n", 2000) + deepMarker + "\n"

	internet := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = io.WriteString(w, bigBody)
	}))
	defer internet.Close()

	storedKey := regexp.MustCompile(`io/[0-9a-f]{16}`)
	var turn atomic.Int32
	var searchFound atomic.Bool
	llm := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/chat/completions") {
			http.NotFound(w, r)
			return
		}
		body, _ := io.ReadAll(r.Body)
		var reply string
		switch turn.Add(1) {
		case 1:
			reply = fmt.Sprintf(`{"actions":[{"action":"core.internet","content":{"method":"GET","url":%q}}]}`, internet.URL+"/page")
		case 2:
			// The page is saved; park on a timer so the search runs in a later
			// quantum.
			reply = `{"actions":[{"action":"sys.timer","content":{"duration_seconds":1,"label":"nap"}}]}`
		case 3:
			key := storedKey.Find(body)
			if key == nil {
				t.Errorf("no saved key in the transcript")
			}
			// The pattern matches the marker without spelling it, so the marker
			// can reach the next turn only through a search hit.
			reply = fmt.Sprintf(`{"actions":[{"action":"core.scratch","content":{"operation":"search","key":%q,"pattern":"SENTINEL_[a-z_]+"}}]}`, key)
		default:
			searchFound.Store(bytes.Contains(body, []byte(deepMarker)))
			reply = `{"actions":[{"action":"final","content":{"answer":"done"}}]}`
		}
		payload, _ := json.Marshal(map[string]any{
			"choices": []any{map[string]any{"message": map[string]any{"content": reply}}},
		})
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(payload)
	}))
	defer llm.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	llmConfig, _ := json.Marshal(map[string]any{
		"base_url":            llm.URL + "/v1",
		"api_key":             map[string]string{"secret": "OPENAI_KEY"},
		"allow_insecure_http": true,
		"default_model":       "stub-model",
		"capabilities":        []map[string]any{{"operation": "chat", "require_approval": false}},
	})
	internetConfig, _ := json.Marshal(map[string]any{
		"capabilities":          []map[string]any{{"methods": []string{"GET"}, "domain": internet.URL}},
		"allow_private_network": true,
		// The whole page, marker included, must reach scratch.
		"max_response_bytes": 1 << 20,
	})
	scratchConfig, _ := json.Marshal(map[string]any{
		"capabilities": []map[string]any{{"operation": "store"}, {"operation": "search"}},
	})
	manifest := aurora.Manifest{
		Version: aurora.ManifestVersion,
		Program: "agent",
		Syscalls: []aurora.Syscall{
			{Syscall: "core.openaiApi", Config: llmConfig, Hidden: true},
			{Syscall: "core.internet", Config: internetConfig},
			{Syscall: "core.scratch", Config: scratchConfig},
		},
	}

	d, err := dist.New(ctx, dist.Config{
		Manifest:    manifest,
		Secrets:     e2eSecrets,
		ProgramsDir: programsDir,
		TaskSecret:  []byte("e2e-secret"),
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("assemble distribution: %v", err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer closeCancel()
		_ = d.Close(closeCtx)
	}()

	server := httptest.NewServer(api.Handler(d, nil))
	defer server.Close()
	c := &client{t: t, base: server.URL, http: server.Client()}

	var session aurora.SessionSnapshot
	c.do(http.MethodPost, "/v1/sessions", map[string]any{}, &session)
	var process aurora.ProcessSnapshot
	c.do(http.MethodPost, "/v1/sessions/"+session.ID+"/processes", map[string]any{
		"input": "research the page",
	}, &process)

	deadline := time.Now().Add(60 * time.Second)
	for process.Status != aurora.ProcessCompleted {
		if process.Status == aurora.ProcessFailed || process.Status == aurora.ProcessStopped {
			t.Fatalf("process finished as %s: %s", process.Status, process.Error)
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out in %s", process.Status)
		}
		time.Sleep(50 * time.Millisecond)
		c.do(http.MethodGet, "/v1/processes/"+process.ID, nil, &process)
	}
	if !searchFound.Load() {
		t.Fatal("the search after the timer did not find the stored page's tail: scratch lost its contents across the quantum boundary")
	}
}

// Scratch is the session's: a process that sees no session history still reads
// what an earlier process of the session stored. Both processes run with
// history:false, so the value can reach the second one only through scratch.
func TestScratchHandsOffBetweenProcessesWithoutHistory(t *testing.T) {
	wasm := buildProgram(t)
	programsDir := t.TempDir()
	writeProgramDir(t, programsDir, wasm)

	const secret = "TEAL-42-handoff"
	var leakedThroughHistory, readBack atomic.Bool
	llm := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/chat/completions") {
			http.NotFound(w, r)
			return
		}
		body, _ := io.ReadAll(r.Body)
		observed := bytes.Contains(body, []byte(`\"status\":\"result\"`))
		var reply string
		switch {
		case bytes.Contains(body, []byte("store the code")) && !observed:
			reply = fmt.Sprintf(`{"actions":[{"action":"core.scratch","content":{"operation":"put","key":"handoff/code","value":%q}}]}`, secret)
		case bytes.Contains(body, []byte("store the code")):
			reply = `{"actions":[{"action":"final","content":{"answer":"stored"}}]}`
		case !observed:
			leakedThroughHistory.Store(bytes.Contains(body, []byte(secret)))
			reply = `{"actions":[{"action":"core.scratch","content":{"operation":"get","key":"handoff/code"}}]}`
		default:
			readBack.Store(bytes.Contains(body, []byte(secret)))
			reply = `{"actions":[{"action":"final","content":{"answer":"recalled"}}]}`
		}
		payload, _ := json.Marshal(map[string]any{
			"choices": []any{map[string]any{"message": map[string]any{"content": reply}}},
		})
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(payload)
	}))
	defer llm.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	llmConfig, _ := json.Marshal(map[string]any{
		"base_url":            llm.URL + "/v1",
		"api_key":             map[string]string{"secret": "OPENAI_KEY"},
		"allow_insecure_http": true,
		"default_model":       "stub-model",
		"capabilities":        []map[string]any{{"operation": "chat", "require_approval": false}},
	})
	scratchConfig, _ := json.Marshal(map[string]any{
		"capabilities": []map[string]any{{"operation": "put"}, {"operation": "get"}},
	})
	noHistory := false
	manifest := aurora.Manifest{
		Version: aurora.ManifestVersion,
		Program: "agent",
		History: &noHistory,
		Syscalls: []aurora.Syscall{
			{Syscall: "core.openaiApi", Config: llmConfig, Hidden: true},
			{Syscall: "core.scratch", Config: scratchConfig},
		},
	}

	d, err := dist.New(ctx, dist.Config{
		Manifest:    manifest,
		Secrets:     e2eSecrets,
		DataDir:     t.TempDir(), // SQLite: the session's scratch is durable
		ProgramsDir: programsDir,
		TaskSecret:  []byte("e2e-secret"),
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("assemble distribution: %v", err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer closeCancel()
		_ = d.Close(closeCtx)
	}()

	server := httptest.NewServer(api.Handler(d, nil))
	defer server.Close()
	c := &client{t: t, base: server.URL, http: server.Client()}

	var session aurora.SessionSnapshot
	c.do(http.MethodPost, "/v1/sessions", map[string]any{}, &session)
	run := func(input string) {
		t.Helper()
		var process aurora.ProcessSnapshot
		c.do(http.MethodPost, "/v1/sessions/"+session.ID+"/processes", map[string]any{
			"input": input,
		}, &process)
		deadline := time.Now().Add(60 * time.Second)
		for process.Status != aurora.ProcessCompleted {
			if process.Status == aurora.ProcessFailed || process.Status == aurora.ProcessStopped {
				t.Fatalf("%q finished as %s: %s", input, process.Status, process.Error)
			}
			if time.Now().After(deadline) {
				t.Fatalf("%q timed out in %s", input, process.Status)
			}
			time.Sleep(50 * time.Millisecond)
			c.do(http.MethodGet, "/v1/processes/"+process.ID, nil, &process)
		}
	}

	run("store the code for later")
	run("recall the code")
	if leakedThroughHistory.Load() {
		t.Fatal("the second process saw the value before reading scratch: history:false did not hide it")
	}
	if !readBack.Load() {
		t.Fatal("the second process did not read the value the first stored in session scratch")
	}
}

// TestAgentSheddingRecoversOversizedRequest proves the guest degrades instead of
// failing when a chat request exceeds the driver's max_request_bytes cap: it
// sheds transcript bytes and retries until the request fits, so the process
// completes. A tiny cap plus a large input forces the shedding path on the very
// first turn.
func TestAgentSheddingRecoversOversizedRequest(t *testing.T) {
	wasm := buildProgram(t)
	programsDir := t.TempDir()
	writeProgramDir(t, programsDir, wasm)

	llm := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/chat/completions") {
			http.NotFound(w, r)
			return
		}
		reply := `{"actions":[{"action":"final","content":{"answer":"done despite the flood"}}]}`
		payload, _ := json.Marshal(map[string]any{
			"choices": []any{map[string]any{"message": map[string]any{"content": reply}}},
		})
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(payload)
	}))
	defer llm.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	llmConfig, _ := json.Marshal(map[string]any{
		"base_url":            llm.URL + "/v1",
		"api_key":             map[string]string{"secret": "OPENAI_KEY"},
		"allow_insecure_http": true,
		"default_model":       "stub-model",
		"max_request_bytes":   8000,
		"capabilities":        []map[string]any{{"operation": "chat", "require_approval": false}},
	})
	manifest := aurora.Manifest{
		Version: aurora.ManifestVersion,
		Program: "agent",
		Syscalls: []aurora.Syscall{
			{Syscall: "core.openaiApi", Config: llmConfig, Hidden: true},
		},
	}

	d, err := dist.New(ctx, dist.Config{
		Manifest:    manifest,
		Secrets:     e2eSecrets,
		ProgramsDir: programsDir,
		TaskSecret:  []byte("e2e-secret"),
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("assemble distribution: %v", err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer closeCancel()
		_ = d.Close(closeCtx)
	}()

	server := httptest.NewServer(api.Handler(d, nil))
	defer server.Close()
	c := &client{t: t, base: server.URL, http: server.Client()}

	// A tiny 8 KB request cap: the first chat request (system prompt + a ~36 KB
	// input) far exceeds it, so the guest must shed to get under it.

	var session aurora.SessionSnapshot
	c.do(http.MethodPost, "/v1/sessions", map[string]any{}, &session)
	var process aurora.ProcessSnapshot
	c.do(http.MethodPost, "/v1/sessions/"+session.ID+"/processes", map[string]any{
		"input": strings.Repeat("flood ", 6000), // ~36 KB, dwarfing the 8 KB cap
	}, &process)

	deadline := time.Now().Add(60 * time.Second)
	for {
		c.do(http.MethodGet, "/v1/processes/"+process.ID, nil, &process)
		if process.Status == aurora.ProcessCompleted {
			break
		}
		if process.Status == aurora.ProcessFailed || process.Status == aurora.ProcessStopped {
			t.Fatalf("process finished as %s: %s (shedding did not recover)", process.Status, process.Error)
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out in %s", process.Status)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if process.Answer != "done despite the flood" {
		t.Fatalf("answer = %q", process.Answer)
	}
}

// Compaction never paraphrases the head: the run's input can carry rules that
// must reach every later turn word for word. When the run's own turns grow past
// the compaction threshold, the summary chat gets only those turns, and the
// next turn still has the input as it was written.
func TestCompactionKeepsTheInput(t *testing.T) {
	wasm := buildProgram(t)
	programsDir := t.TempDir()
	writeProgramDir(t, programsDir, wasm)

	const rule = "SAFETY-RULE-7731: never delete anything"
	// Each reply carries ~150 KB of padding, so four turns pass the 512 KB
	// compaction threshold.
	padding := strings.Repeat("x", 150*1024)
	var compacted, summarizerSawRule, keptRule atomic.Bool
	llm := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/chat/completions") {
			http.NotFound(w, r)
			return
		}
		body, _ := io.ReadAll(r.Body)
		var reply string
		switch {
		case bytes.Contains(body, []byte("compress the earlier work log of an AI agent")):
			summarizerSawRule.Store(bytes.Contains(body, []byte(rule)))
			reply = "The agent listed its notes several times."
		case bytes.Contains(body, []byte("[A summary of the earlier steps")):
			compacted.Store(true)
			keptRule.Store(bytes.Contains(body, []byte(rule)))
			reply = `{"actions":[{"action":"final","content":{"answer":"done"}}]}`
		default:
			reply = fmt.Sprintf(`{"actions":[{"action":"core.scratch","content":{"operation":"list"}}],"padding":%q}`, padding)
		}
		payload, _ := json.Marshal(map[string]any{
			"choices": []any{map[string]any{"message": map[string]any{"content": reply}}},
		})
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(payload)
	}))
	defer llm.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	llmConfig, _ := json.Marshal(map[string]any{
		"base_url":            llm.URL + "/v1",
		"api_key":             map[string]string{"secret": "OPENAI_KEY"},
		"allow_insecure_http": true,
		"default_model":       "stub-model",
		"capabilities":        []map[string]any{{"operation": "chat", "require_approval": false}},
	})
	scratchConfig, _ := json.Marshal(map[string]any{
		"capabilities": []map[string]any{{"operation": "list"}},
	})
	manifest := aurora.Manifest{
		Version: aurora.ManifestVersion,
		Program: "agent",
		Syscalls: []aurora.Syscall{
			{Syscall: "core.openaiApi", Config: llmConfig, Hidden: true},
			{Syscall: "core.scratch", Config: scratchConfig},
		},
	}

	d, err := dist.New(ctx, dist.Config{
		Manifest:    manifest,
		Secrets:     e2eSecrets,
		ProgramsDir: programsDir,
		TaskSecret:  []byte("e2e-secret"),
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("assemble distribution: %v", err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer closeCancel()
		_ = d.Close(closeCtx)
	}()

	server := httptest.NewServer(api.Handler(d, nil))
	defer server.Close()
	c := &client{t: t, base: server.URL, http: server.Client()}

	var session aurora.SessionSnapshot
	c.do(http.MethodPost, "/v1/sessions", map[string]any{}, &session)
	var process aurora.ProcessSnapshot
	c.do(http.MethodPost, "/v1/sessions/"+session.ID+"/processes", map[string]any{
		"input": rule + ". List the notes until you are told to stop.",
	}, &process)

	deadline := time.Now().Add(60 * time.Second)
	for process.Status != aurora.ProcessCompleted {
		if process.Status == aurora.ProcessFailed || process.Status == aurora.ProcessStopped {
			t.Fatalf("process finished as %s: %s", process.Status, process.Error)
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out in %s", process.Status)
		}
		time.Sleep(50 * time.Millisecond)
		c.do(http.MethodGet, "/v1/processes/"+process.ID, nil, &process)
	}
	if !compacted.Load() {
		t.Fatal("the transcript was never compacted")
	}
	if summarizerSawRule.Load() {
		t.Fatal("the input went into the compaction summary")
	}
	if !keptRule.Load() {
		t.Fatal("the turn after compaction lost the input")
	}
}

// runAgent runs one agent process against a stub LLM, with the LLM grant and
// the extra grants given, and returns the process when it ends. output is the
// program's output schema; empty means a plain string.
func runAgent(t *testing.T, output string, llm http.HandlerFunc, extra ...aurora.Syscall) aurora.ProcessSnapshot {
	t.Helper()
	wasm := buildProgram(t)
	programsDir := t.TempDir()
	writeProgramDir(t, programsDir, wasm)
	if output != "" {
		iface := `{"description":"the agent","input":{"type":"string"},"output":` + output + `}`
		if err := os.WriteFile(filepath.Join(programsDir, "agent.json"), []byte(iface), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	stub := httptest.NewServer(llm)
	defer stub.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	manifest := testManifest(stub.URL + "/v1")
	manifest.Syscalls = append(manifest.Syscalls, extra...)

	d, err := dist.New(ctx, dist.Config{
		Manifest:    manifest,
		Secrets:     e2eSecrets,
		ProgramsDir: programsDir,
		TaskSecret:  []byte("e2e-secret"),
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("assemble distribution: %v", err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer closeCancel()
		_ = d.Close(closeCtx)
	}()
	server := httptest.NewServer(api.Handler(d, nil))
	defer server.Close()
	c := &client{t: t, base: server.URL, http: server.Client()}

	var session aurora.SessionSnapshot
	c.do(http.MethodPost, "/v1/sessions", map[string]any{}, &session)
	var process aurora.ProcessSnapshot
	c.do(http.MethodPost, "/v1/sessions/"+session.ID+"/processes", map[string]any{
		"input": "do the task",
	}, &process)
	deadline := time.Now().Add(60 * time.Second)
	for process.Status != aurora.ProcessCompleted && process.Status != aurora.ProcessFailed {
		if time.Now().After(deadline) {
			t.Fatalf("timed out in %s", process.Status)
		}
		time.Sleep(50 * time.Millisecond)
		c.do(http.MethodGet, "/v1/processes/"+process.ID, nil, &process)
	}
	return process
}

// chatReply writes an OpenAI-style chat completion with content as the reply.
func chatReply(w http.ResponseWriter, content string) {
	payload, _ := json.Marshal(map[string]any{
		"choices": []any{map[string]any{"message": map[string]any{"content": content}}},
	})
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(payload)
}

// A request for a tool the run does not have goes back to the model as a failed
// observation, and the model can correct itself: the process does not fail.
func TestAgentReportsAnUnknownTool(t *testing.T) {
	process := runAgent(t, "", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if bytes.Contains(body, []byte("unknown tool")) {
			chatReply(w, `{"actions":[{"action":"final","content":{"answer":"corrected"}}]}`)
			return
		}
		chatReply(w, `{"actions":[{"action":"missing.tool","content":{}}]}`)
	})
	if process.Status != aurora.ProcessCompleted || process.Answer != "corrected" {
		t.Fatalf("process = %s %q (%s), want completed with the corrected answer", process.Status, process.Answer, process.Error)
	}
}

func TestSkillsAndSysToolsReachTheModel(t *testing.T) {
	var prompt atomic.Value
	process := runAgent(t, "", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		prompt.Store(string(body))
		chatReply(w, `{"actions":[{"action":"final","content":{"answer":"done"}}]}`)
	})
	if process.Status != aurora.ProcessCompleted {
		t.Fatalf("process = %s (%s)", process.Status, process.Error)
	}
	seen, _ := prompt.Load().(string)
	for _, want := range []string{"Skill: duty", "Name the service you checked in every answer.", "Name: sys.timer", "Name: sys.declassify"} {
		if !strings.Contains(seen, want) {
			t.Fatalf("the model's request lacks %q:\n%s", want, seen)
		}
	}
}

// The model opens a section with sys.begin and abandons it with sys.abort: the
// run goes back to the section's start, the model decides again, and the
// process finishes in the forked revision.
func TestAgentSectionAbortRunsTheSectionAgain(t *testing.T) {
	var decisions atomic.Int32
	process := runAgent(t, "", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		// The prompt names sys.begin too, so look for its observation.
		if !bytes.Contains(body, []byte(`\"action\":\"sys.begin\",\"status\":\"result\"`)) {
			chatReply(w, `{"actions":[{"action":"sys.begin","content":{}}]}`)
			return
		}
		// Inside the section: abandon it once, then finish.
		if decisions.Add(1) == 1 {
			chatReply(w, `{"actions":[{"action":"sys.abort","content":{"reason":"try another way"}}]}`)
			return
		}
		chatReply(w, `{"actions":[{"action":"final","content":{"answer":"second try"}}]}`)
	})
	if process.Status != aurora.ProcessCompleted || process.Answer != "second try" {
		t.Fatalf("process = %s %q (%s), want completed by the second try", process.Status, process.Answer, process.Error)
	}
	if process.Revision != 2 {
		t.Fatalf("revision = %d, want 2: the abort forks the section", process.Revision)
	}
}

// An answer that does not fit the program's output schema goes back to the
// model with the reason, and the model answers again: the process does not
// fail.
func TestAgentCorrectsARejectedAnswer(t *testing.T) {
	process := runAgent(t, `{"type":"object","required":["status"]}`, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if bytes.Contains(body, []byte("did not accept your final answer")) {
			chatReply(w, `{"actions":[{"action":"final","content":{"answer":"{\"status\":\"ok\"}"}}]}`)
			return
		}
		chatReply(w, `{"actions":[{"action":"final","content":{"answer":"all good"}}]}`)
	})
	if process.Status != aurora.ProcessCompleted || process.Answer != `{"status":"ok"}` {
		t.Fatalf("process = %s %q (%s), want completed with the corrected answer", process.Status, process.Answer, process.Error)
	}
}
