package api_test

// The distribution end-to-end: the real Rust agent program (built from the
// sibling aurora-brains checkout) driven through the real HTTP API, with an
// OpenAI-compatible stub as cognition. The scripted model first sets a
// one-second timer — exercising the durable-task path and the distribution's
// timer reconcile/firing loop — then finishes. Everything is observed the way
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

func testManifest(llmBaseURL string) aurora.Manifest {
	config, _ := json.Marshal(map[string]any{
		"base_url":            llmBaseURL,
		"api_key":             "test-key",
		"allow_insecure_http": true,
		"default_model":       "stub-model",
		"capabilities": []map[string]any{
			{"operation": "chat", "require_approval": false},
		},
	})
	return aurora.Manifest{
		Version: aurora.ManifestVersion,
		Syscalls: []aurora.Syscall{
			{Syscall: "sys.timer"},
			{Syscall: "core.openaiApi", Config: config, Hidden: true},
		},
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

	server := httptest.NewServer(api.Handler(d))
	defer server.Close()
	c := &client{t: t, base: server.URL, http: server.Client()}

	// The loaded program set is readable — the terminal's `ls /programs` — and
	// each artifact carries the interface it declares (description + input/output
	// schemas), read from the sidecar manifest beside the wasm at load.
	var artifacts []aurora.ProgramArtifact
	c.do(http.MethodGet, "/v1/programs", nil, &artifacts)
	if len(artifacts) != 1 || artifacts[0].ID != "agent" || artifacts[0].Digest == "" {
		t.Fatalf("programs = %+v, want the loaded agent artifact with its digest", artifacts)
	}
	if artifacts[0].Description == "" || len(artifacts[0].Input) == 0 || len(artifacts[0].Output) == 0 {
		t.Fatalf("program interface not surfaced: %+v", artifacts[0])
	}

	// Create a session, start a process.
	var session dist.SessionLog
	c.do(http.MethodPost, "/v1/sessions", map[string]any{"tags": map[string]string{"origin": "e2e"}}, &session)
	if !strings.HasPrefix(session.Session.ID, "ses_") {
		t.Fatalf("session id = %q", session.Session.ID)
	}
	sessionID := session.Session.ID
	var process aurora.ProcessSnapshot
	c.do(http.MethodPost, "/v1/sessions/"+sessionID+"/processes", map[string]any{
		"input":    "take a nap, then report back",
		"manifest": testManifest(llm.URL + "/v1"),
	}, &process)
	if !strings.HasPrefix(process.ID, "proc_") || process.SessionID != sessionID {
		t.Fatalf("process = %+v", process)
	}

	// The process sets a 1s timer (a durable task) which the distribution's
	// timer service fires; then the model finishes.
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
	if len(session.Processes) != 1 || session.Session.ActiveProcessID != "" || len(session.History) != 2 {
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

	// The timer task resolved as completed by the timer actor.
	tasks := logged.Tasks
	if len(tasks) != 1 || tasks[0].State != aurora.TaskStateExecuted && tasks[0].State != aurora.TaskStateCompleted {
		t.Fatalf("tasks = %+v", tasks)
	}
	if tasks[0].Resolution.Actor != "timer" {
		t.Fatalf("timer task resolved by %q", tasks[0].Resolution.Actor)
	}
}

// A distribution restart mid-wait: the process parks on a durable timer task
// in SQLite, the whole distribution shuts down, and a fresh assembly restores
// the session, re-arms the elapsed timer, fires it, and drives the process to
// completion — drain-and-restart with no live state carried over.
func TestDistributionRestartRecoversTimers(t *testing.T) {
	wasm := buildProgram(t)

	programsDir := t.TempDir()
	writeProgramDir(t, programsDir, wasm)
	dataDir := t.TempDir()
	llm := scriptedLLM(t)
	defer llm.Close()

	config := dist.Config{
		DataDir:     dataDir,
		ProgramsDir: programsDir,
		TaskSecret:  []byte("e2e-secret"),
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	first, err := dist.New(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(api.Handler(first))
	c := &client{t: t, base: server.URL, http: server.Client()}

	var session dist.SessionLog
	c.do(http.MethodPost, "/v1/sessions", nil, &session)
	sessionID := session.Session.ID
	var process aurora.ProcessSnapshot
	c.do(http.MethodPost, "/v1/sessions/"+sessionID+"/processes", map[string]any{
		"input":    "take a nap, then report back",
		"manifest": testManifest(llm.URL + "/v1"),
	}, &process)

	// Wait for the park on the timer task, then kill the first instance
	// before the timer can fire.
	deadline := time.Now().Add(30 * time.Second)
	for process.Status != aurora.ProcessWaitingTask {
		if time.Now().After(deadline) {
			t.Fatalf("never parked; status %s", process.Status)
		}
		time.Sleep(20 * time.Millisecond)
		c.do(http.MethodGet, "/v1/processes/"+process.ID, nil, &process)
	}
	first.Timers.StopAll() // simulate dying before the fire
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
	server = httptest.NewServer(api.Handler(second))
	defer server.Close()
	c = &client{t: t, base: server.URL, http: server.Client()}

	// Recovery re-armed the elapsed timer, fired it immediately, and the
	// process finished on the new instance.
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
		DataDir:     dataDir,
		ProgramsDir: programsDir,
		TaskSecret:  []byte("e2e-secret"),
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	first, err := dist.New(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(api.Handler(first))
	c := &client{t: t, base: server.URL, http: server.Client()}

	var session dist.SessionLog
	c.do(http.MethodPost, "/v1/sessions", nil, &session)
	var process aurora.ProcessSnapshot
	c.do(http.MethodPost, "/v1/sessions/"+session.Session.ID+"/processes", map[string]any{
		"input":    "do the thing",
		"manifest": testManifest(llm.URL + "/v1"),
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
	server = httptest.NewServer(api.Handler(second))
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

// The ceiling refuses over-grants at the door with a 400, before the runtime
// sees the manifest.
func TestCapabilityCeilingOverHTTP(t *testing.T) {
	ctx := context.Background()
	d, err := dist.New(ctx, dist.Config{
		TaskSecret:        []byte("e2e-secret"),
		CapabilityCeiling: []string{"sys.timer"},
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close(ctx)
	server := httptest.NewServer(api.Handler(d))
	defer server.Close()

	var session dist.SessionLog
	c := &client{t: t, base: server.URL, http: server.Client()}
	c.do(http.MethodPost, "/v1/sessions", nil, &session)

	body, _ := json.Marshal(map[string]any{
		"input": "hi",
		"manifest": aurora.Manifest{Version: aurora.ManifestVersion, Syscalls: []aurora.Syscall{
			{Syscall: "core.internet", Config: json.RawMessage(`{"capabilities":[{"methods":["GET"],"domain":"example.com"}]}`)},
		}},
	})
	resp, err := http.Post(server.URL+"/v1/sessions/"+session.Session.ID+"/processes", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusBadRequest || !strings.Contains(string(raw), "core.internet") {
		t.Fatalf("status = %d body = %s, want 400 naming the capability", resp.StatusCode, raw)
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
		reply := `{"actions":[{"action":"core.memory","content":{"operation":"get","key":"k"}}]}`
		payload, _ := json.Marshal(map[string]any{
			"choices": []any{map[string]any{"message": map[string]any{"content": reply}}},
		})
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(payload)
	}))
	defer llm.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d, err := dist.New(ctx, dist.Config{
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

	server := httptest.NewServer(api.Handler(d))
	defer server.Close()
	c := &client{t: t, base: server.URL, http: server.Client()}

	llmConfig, _ := json.Marshal(map[string]any{
		"base_url":            llm.URL + "/v1",
		"api_key":             "test-key",
		"allow_insecure_http": true,
		"default_model":       "stub-model",
		"capabilities":        []map[string]any{{"operation": "chat", "require_approval": false}},
	})
	memConfig, _ := json.Marshal(map[string]any{
		"capabilities": []map[string]any{{"operation": "get"}},
	})
	manifest := aurora.Manifest{
		Version: aurora.ManifestVersion,
		Syscalls: []aurora.Syscall{
			{Syscall: "core.openaiApi", Config: llmConfig, Hidden: true},
			{Syscall: "core.memory", Config: memConfig},
		},
	}

	var session dist.SessionLog
	c.do(http.MethodPost, "/v1/sessions", map[string]any{}, &session)
	var process aurora.ProcessSnapshot
	c.do(http.MethodPost, "/v1/sessions/"+session.Session.ID+"/processes", map[string]any{
		"input":    "keep going forever",
		"manifest": manifest,
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
	d, err := dist.New(ctx, dist.Config{
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

	server := httptest.NewServer(api.Handler(d))
	defer server.Close()
	c := &client{t: t, base: server.URL, http: server.Client()}

	llmConfig, _ := json.Marshal(map[string]any{
		"base_url":            llm.URL + "/v1",
		"api_key":             "test-key",
		"allow_insecure_http": true,
		"default_model":       "stub-model",
		"capabilities":        []map[string]any{{"operation": "chat", "require_approval": false}},
	})
	manifest := aurora.Manifest{
		Version: aurora.ManifestVersion,
		Syscalls: []aurora.Syscall{
			{Syscall: "core.openaiApi", Config: llmConfig, Hidden: true},
		},
	}

	var session dist.SessionLog
	c.do(http.MethodPost, "/v1/sessions", map[string]any{}, &session)
	var process aurora.ProcessSnapshot
	c.do(http.MethodPost, "/v1/sessions/"+session.Session.ID+"/processes", map[string]any{
		"input":    "is warp a good terminal emulator?",
		"manifest": manifest,
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

// TestAgentOffloadsLargeInternetRead proves a large fetched body is offloaded to
// the store instead of inlined into the transcript: after a GET returns a body
// well past the offload threshold, the model's next turn sees a reference
// (stored_key + summary + a short excerpt), never the full body — so a deep tail
// marker that sits past the excerpt can only reach the model if offloading
// failed.
func TestAgentOffloadsLargeInternetRead(t *testing.T) {
	wasm := buildProgram(t)
	programsDir := t.TempDir()
	writeProgramDir(t, programsDir, wasm)

	const deepMarker = "DEEP_BODY_SENTINEL_should_never_reach_the_model"
	// ~110 KB, past the 48 KB offload threshold; the marker sits at the very end,
	// well beyond the 2 KB head excerpt.
	bigBody := strings.Repeat("lorem ipsum dolor sit amet consectetur adipiscing elit. ", 2000) + deepMarker

	internet := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = io.WriteString(w, bigBody)
	}))
	defer internet.Close()

	var sawStoredKey, leaked atomic.Bool
	llm := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/chat/completions") {
			http.NotFound(w, r)
			return
		}
		body, _ := io.ReadAll(r.Body)
		var reply string
		switch {
		case bytes.Contains(body, []byte("compress a web page")):
			// The fetch summarizer — a self-contained chat; reply with prose.
			reply = "The page is placeholder lorem-ipsum text with no substantive content."
		case bytes.Contains(body, []byte("stored_key")):
			// The agent's turn after the offload: it must see the reference, not
			// the full body.
			sawStoredKey.Store(true)
			if bytes.Contains(body, []byte(deepMarker)) {
				leaked.Store(true)
			}
			reply = `{"actions":[{"action":"final","content":{"answer":"done"}}]}`
		default:
			reply = fmt.Sprintf(`{"actions":[{"action":"core.internet","content":{"method":"GET","url":%q}}]}`, internet.URL+"/page")
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
	d, err := dist.New(ctx, dist.Config{
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

	server := httptest.NewServer(api.Handler(d))
	defer server.Close()
	c := &client{t: t, base: server.URL, http: server.Client()}

	llmConfig, _ := json.Marshal(map[string]any{
		"base_url":            llm.URL + "/v1",
		"api_key":             "test-key",
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
	})
	// Offload lands in core.scratch — process-local and ephemeral, never the
	// durable tenant memory.
	scratchConfig, _ := json.Marshal(map[string]any{
		"capabilities": []map[string]any{{"operation": "put"}, {"operation": "get"}, {"operation": "search"}},
	})
	manifest := aurora.Manifest{
		Version: aurora.ManifestVersion,
		Syscalls: []aurora.Syscall{
			{Syscall: "core.openaiApi", Config: llmConfig, Hidden: true},
			{Syscall: "core.internet", Config: internetConfig},
			{Syscall: "core.scratch", Config: scratchConfig},
		},
	}

	var session dist.SessionLog
	c.do(http.MethodPost, "/v1/sessions", map[string]any{}, &session)
	var process aurora.ProcessSnapshot
	c.do(http.MethodPost, "/v1/sessions/"+session.Session.ID+"/processes", map[string]any{
		"input":    "research the page",
		"manifest": manifest,
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

	if !sawStoredKey.Load() {
		t.Fatal("the model never saw an offloaded observation (stored_key) — offload did not trigger")
	}
	if leaked.Load() {
		t.Fatal("the full response body reached the model — it was inlined, not offloaded")
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
	d, err := dist.New(ctx, dist.Config{
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

	server := httptest.NewServer(api.Handler(d))
	defer server.Close()
	c := &client{t: t, base: server.URL, http: server.Client()}

	// A tiny 8 KB request cap: the first chat request (system prompt + a ~36 KB
	// input) far exceeds it, so the guest must shed to get under it.
	llmConfig, _ := json.Marshal(map[string]any{
		"base_url":            llm.URL + "/v1",
		"api_key":             "test-key",
		"allow_insecure_http": true,
		"default_model":       "stub-model",
		"max_request_bytes":   8000,
		"capabilities":        []map[string]any{{"operation": "chat", "require_approval": false}},
	})
	manifest := aurora.Manifest{
		Version: aurora.ManifestVersion,
		Syscalls: []aurora.Syscall{
			{Syscall: "core.openaiApi", Config: llmConfig, Hidden: true},
		},
	}

	var session dist.SessionLog
	c.do(http.MethodPost, "/v1/sessions", map[string]any{}, &session)
	var process aurora.ProcessSnapshot
	c.do(http.MethodPost, "/v1/sessions/"+session.Session.ID+"/processes", map[string]any{
		"input":    strings.Repeat("flood ", 6000), // ~36 KB, dwarfing the 8 KB cap
		"manifest": manifest,
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
