package api_test

// The distribution end-to-end: the real Rust agent program (built from the
// sibling aurora-brains checkout) driven through the real HTTP+SSE API, with
// an OpenAI-compatible stub as cognition. The scripted model first sets a
// one-second timer — exercising the durable-task path and the distribution's
// timer firing loop — then finishes. Everything is observed the way a
// terminal would observe it: REST snapshots, the journal, and the tenant
// firehose.

import (
	"bufio"
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
	"testing"
	"time"

	"github.com/aurora-capcompute/aurora-capcompute/aurora"

	"github.com/aurora-capcompute/aurora-dist/internal/api"
	"github.com/aurora-capcompute/aurora-dist/internal/dist"
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
			"-p", "agent-brain",
		)
		cmd.Dir = "../../../aurora-brains"
		if out, err := cmd.CombinedOutput(); err != nil {
			programError = fmt.Errorf("build program: %v\n%s", err, out)
			return
		}
		wasmPath := filepath.Join(cmd.Dir, "target", "wasm32-wasip1", "release", "agent_brain.wasm")
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

// scriptedLLM is an OpenAI-compatible chat stub: until it has seen a timer
// observation it asks for timer.set; afterwards it finishes.
func scriptedLLM(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/chat/completions") {
			http.NotFound(w, r)
			return
		}
		body, _ := io.ReadAll(r.Body)
		reply := `{"actions":[{"action":"timer.set","content":{"duration_seconds":1,"label":"nap"}}]}`
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
	settings, _ := json.Marshal(map[string]any{
		"base_url":            llmBaseURL,
		"api_key":             "test-key",
		"allow_insecure_http": true,
		"default_model":       "stub-model",
		"require_approval":    false,
	})
	return aurora.Manifest{
		Version: aurora.ManifestVersion,
		Tools: []aurora.Tool{
			{Name: "timer.set", Type: "core.timer"},
			{Name: "llm", Type: "core.openaiApi", Settings: settings, Hidden: true},
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

// collectSSE reads one SSE stream, sending each event name to the channel.
func collectSSE(t *testing.T, url string, events chan<- string, done <-chan struct{}) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Error(err)
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() { <-done; cancel() }()
	resp, err := http.DefaultClient.Do(req.WithContext(ctx))
	if err != nil {
		return
	}
	defer resp.Body.Close()
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "event: ") {
			select {
			case events <- strings.TrimPrefix(line, "event: "):
			case <-ctx.Done():
				return
			}
		}
	}
}

func TestDistributionEndToEnd(t *testing.T) {
	wasm := buildProgram(t)

	programsDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(programsDir, "agent.wasm"), wasm, 0o600); err != nil {
		t.Fatal(err)
	}
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

	// The program registry loaded the artifact from the directory.
	var artifacts []aurora.ProgramArtifact
	c.do(http.MethodGet, "/v1/programs", nil, &artifacts)
	if len(artifacts) != 1 || artifacts[0].ID != "agent" || artifacts[0].Digest == "" {
		t.Fatalf("programs = %+v", artifacts)
	}

	// Attach the tenant firehose before anything happens.
	firehoseEvents := make(chan string, 256)
	firehoseDone := make(chan struct{})
	defer close(firehoseDone)
	go collectSSE(t, server.URL+"/v1/events", firehoseEvents, firehoseDone)
	time.Sleep(50 * time.Millisecond) // let the stream attach

	// Create a session, start a process.
	var session aurora.SessionSnapshot
	c.do(http.MethodPost, "/v1/sessions", map[string]any{"tags": map[string]string{"origin": "e2e"}}, &session)
	if !strings.HasPrefix(session.ID, "ses_") {
		t.Fatalf("session id = %q", session.ID)
	}
	var process aurora.ProcessSnapshot
	c.do(http.MethodPost, "/v1/sessions/"+session.ID+"/processes", map[string]any{
		"message":  "take a nap, then report back",
		"manifest": testManifest(llm.URL + "/v1"),
	}, &process)
	if !strings.HasPrefix(process.ID, "proc_") || process.SessionID != session.ID {
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

	// The journal narrates the whole story: input → chat → timer → chat → finish.
	var journal []aurora.JournalEntry
	c.do(http.MethodGet, "/v1/processes/"+process.ID+"/journal", nil, &journal)
	var names []string
	for _, entry := range journal {
		names = append(names, entry.Syscall.Name)
	}
	story := strings.Join(names, " ")
	for _, want := range []string{"agent.input", "openai.chat", "timer.set", "agent.finish"} {
		if !strings.Contains(story, want) {
			t.Fatalf("journal %v is missing %s", names, want)
		}
	}

	// The timer task resolved as completed by the timer actor.
	var tasks []aurora.TaskSnapshot
	c.do(http.MethodGet, "/v1/processes/"+process.ID+"/tasks", nil, &tasks)
	if len(tasks) != 1 || tasks[0].State != aurora.TaskStateExecuted && tasks[0].State != aurora.TaskStateCompleted {
		t.Fatalf("tasks = %+v", tasks)
	}
	if tasks[0].Resolution.Actor != "timer" {
		t.Fatalf("timer task resolved by %q", tasks[0].Resolution.Actor)
	}

	// The session snapshot folded the conversation.
	c.do(http.MethodGet, "/v1/sessions/"+session.ID, nil, &session)
	if len(session.Processes) != 1 || session.ActiveProcessID != "" || len(session.History) != 2 {
		t.Fatalf("session = %+v", session)
	}

	// Retention: the digest is pinned by no non-terminal process now.
	var retention []struct {
		Digest           string   `json:"digest"`
		Programs         []string `json:"programs"`
		Processes        []string `json:"processes"`
		Decommissionable bool     `json:"decommissionable"`
	}
	c.do(http.MethodGet, "/v1/programs/retention", nil, &retention)
	if len(retention) != 1 || !retention[0].Decommissionable || retention[0].Digest != artifacts[0].Digest {
		t.Fatalf("retention = %+v", retention)
	}

	// The firehose saw the lifecycle: session.created, process updates, the
	// timer task, journal appends.
	wanted := map[string]bool{
		"session.created": false, "process.updated": false,
		"task.created": false, "journal.appended": false,
	}
	seen := map[string]bool{}
	drain := time.After(5 * time.Second)
	for {
		missing := 0
		for name, ok := range wanted {
			if !ok && seen[name] {
				wanted[name] = true
			} else if !wanted[name] {
				missing++
			}
		}
		if missing == 0 {
			break
		}
		select {
		case name := <-firehoseEvents:
			seen[name] = true
		case <-drain:
			t.Fatalf("firehose events seen: %v, want all of %v", seen, wanted)
		}
	}

	// Firehose resume: an in-window cursor replays without a snapshot.
	resp, err := http.Get(server.URL + "/v1/events?after=1")
	if err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(resp.Body)
	line, err := reader.ReadString('\n')
	resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(line, "id: 2") {
		t.Fatalf("resume first line = %q, want replay from seq 2", line)
	}
}

// A distribution restart mid-wait: the process parks on a durable timer task
// in SQLite, the whole distribution shuts down, and a fresh assembly restores
// the session, re-arms the elapsed timer, fires it, and drives the process to
// completion — drain-and-restart with no live state carried over.
func TestDistributionRestartRecoversTimers(t *testing.T) {
	wasm := buildProgram(t)

	programsDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(programsDir, "agent.wasm"), wasm, 0o600); err != nil {
		t.Fatal(err)
	}
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

	var session aurora.SessionSnapshot
	c.do(http.MethodPost, "/v1/sessions", nil, &session)
	var process aurora.ProcessSnapshot
	c.do(http.MethodPost, "/v1/sessions/"+session.ID+"/processes", map[string]any{
		"message":  "take a nap, then report back",
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

// The ceiling refuses over-grants at the door with a 400, before the runtime
// sees the manifest.
func TestCapabilityCeilingOverHTTP(t *testing.T) {
	ctx := context.Background()
	d, err := dist.New(ctx, dist.Config{
		TaskSecret:        []byte("e2e-secret"),
		CapabilityCeiling: []string{"timer.set"},
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close(ctx)
	server := httptest.NewServer(api.Handler(d))
	defer server.Close()

	var session aurora.SessionSnapshot
	c := &client{t: t, base: server.URL, http: server.Client()}
	c.do(http.MethodPost, "/v1/sessions", nil, &session)

	body, _ := json.Marshal(map[string]any{
		"message": "hi",
		"manifest": aurora.Manifest{Version: aurora.ManifestVersion, Tools: []aurora.Tool{
			{Name: "fetch", Type: "core.internet", Settings: json.RawMessage(`{"permissions":[{"requestType":"GET","domain":"example.com"}]}`)},
		}},
	})
	resp, err := http.Post(server.URL+"/v1/sessions/"+session.ID+"/processes", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusBadRequest || !strings.Contains(string(raw), "fetch") {
		t.Fatalf("status = %d body = %s, want 400 naming the capability", resp.StatusCode, raw)
	}
}
