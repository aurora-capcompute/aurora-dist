// Package api serves the distribution's HTTP+SSE API — the single way into
// the runtime, versioned /v1 from birth (the resolution_token and session_id
// renames were the cautionary tales). It is a thin projection of the runtime
// surface plus the distribution's own services: the tenant event firehose,
// the program registry with its retention query, and the capability ceiling
// enforced at process creation.
//
// There is no principal authentication here by design: the distribution
// serves one trusted client (a local terminal, or the policy layer once
// multi-principal). Task resolution still authenticates its bearer
// resolution_token — that credential gates a specific pending decision, not
// API access.
package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/aurora-capcompute/aurora-capcompute/aurora"

	"github.com/aurora-capcompute/aurora-dist/internal/dist"
)

// Handler serves the /v1 API over an assembled distribution.
func Handler(d *dist.Dist) http.Handler {
	h := &handler{dist: d}
	mux := http.NewServeMux()

	// The tenant firehose: one stream for the whole tenant, resumable.
	mux.HandleFunc("GET /v1/events", h.firehose)

	// Sessions.
	mux.HandleFunc("GET /v1/sessions", h.listSessions)
	mux.HandleFunc("POST /v1/sessions", h.createSession)
	mux.HandleFunc("GET /v1/sessions/{id}", h.getSession)
	mux.HandleFunc("GET /v1/sessions/{id}/graph", h.sessionGraph)
	mux.HandleFunc("GET /v1/sessions/{id}/events", h.sessionEvents)
	mux.HandleFunc("POST /v1/sessions/{id}/processes", h.createProcess)

	// Processes.
	mux.HandleFunc("GET /v1/processes/{id}", h.getProcess)
	mux.HandleFunc("GET /v1/processes/{id}/graph", h.processGraph)
	mux.HandleFunc("GET /v1/processes/{id}/journal", h.processJournal)
	mux.HandleFunc("GET /v1/processes/{id}/journal/revisions", h.processJournalRevisions)
	mux.HandleFunc("GET /v1/processes/{id}/tasks", h.processTasks)
	mux.HandleFunc("POST /v1/processes/{id}/stop", h.stopProcess)
	mux.HandleFunc("POST /v1/processes/{id}/retry", h.retryProcess)

	// Tasks.
	mux.HandleFunc("POST /v1/tasks/{id}/resolve", h.resolveTask)

	// Programs.
	mux.HandleFunc("GET /v1/programs", h.listPrograms)
	mux.HandleFunc("POST /v1/programs/reload", h.reloadPrograms)
	mux.HandleFunc("GET /v1/programs/retention", h.programRetention)

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	return mux
}

type handler struct {
	dist *dist.Dist
}

// --- sessions ---

func (h *handler) listSessions(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, h.dist.Runtime.ListSessions(), nil)
}

type createSessionRequest struct {
	Tags map[string]string `json:"tags,omitempty"`
}

func (h *handler) createSession(w http.ResponseWriter, r *http.Request) {
	var req createSessionRequest
	if r.ContentLength != 0 && !readJSON(w, r, &req) {
		return
	}
	snapshot, err := h.dist.CreateSession(req.Tags)
	writeJSON(w, snapshot, err)
}

func (h *handler) getSession(w http.ResponseWriter, r *http.Request) {
	snapshot, err := h.dist.Runtime.GetSession(r.PathValue("id"))
	writeJSON(w, snapshot, err)
}

func (h *handler) sessionGraph(w http.ResponseWriter, r *http.Request) {
	graph, err := h.dist.Runtime.SessionGraph(r.PathValue("id"))
	writeJSON(w, graph, err)
}

// --- processes ---

type createProcessRequest struct {
	Message string `json:"message"`
	// Manifest arrives per-process from the client — there is deliberately no
	// manifest entity in the core. Omitted means an empty composition (no
	// tools) at the current manifest version.
	Manifest *aurora.Manifest `json:"manifest,omitempty"`
}

func (h *handler) createProcess(w http.ResponseWriter, r *http.Request) {
	var req createProcessRequest
	if !readJSON(w, r, &req) {
		return
	}
	manifest := aurora.Manifest{Version: aurora.ManifestVersion}
	if req.Manifest != nil {
		manifest = *req.Manifest
	}
	snapshot, err := h.dist.CreateProcess(r.PathValue("id"), req.Message, manifest)
	writeJSON(w, snapshot, err)
}

func (h *handler) getProcess(w http.ResponseWriter, r *http.Request) {
	snapshot, err := h.dist.Runtime.GetProcess(r.PathValue("id"))
	writeJSON(w, snapshot, err)
}

func (h *handler) processGraph(w http.ResponseWriter, r *http.Request) {
	graph, err := h.dist.Runtime.CallGraph(r.PathValue("id"))
	writeJSON(w, graph, err)
}

func (h *handler) processJournal(w http.ResponseWriter, r *http.Request) {
	entries, err := h.dist.Runtime.Journal(r.PathValue("id"))
	writeJSON(w, entries, err)
}

func (h *handler) processJournalRevisions(w http.ResponseWriter, r *http.Request) {
	revisions, err := h.dist.Runtime.JournalRevisions(r.PathValue("id"))
	writeJSON(w, revisions, err)
}

func (h *handler) processTasks(w http.ResponseWriter, r *http.Request) {
	tasks, err := h.dist.Runtime.Tasks(r.PathValue("id"))
	writeJSON(w, tasks, err)
}

func (h *handler) stopProcess(w http.ResponseWriter, r *http.Request) {
	snapshot, err := h.dist.Runtime.Stop(r.PathValue("id"))
	writeJSON(w, snapshot, err)
}

type retryRequest struct {
	Mode aurora.RetryMode `json:"mode"`
}

func (h *handler) retryProcess(w http.ResponseWriter, r *http.Request) {
	var req retryRequest
	if !readJSON(w, r, &req) {
		return
	}
	snapshot, err := h.dist.Runtime.Retry(r.PathValue("id"), req.Mode)
	writeJSON(w, snapshot, err)
}

// --- tasks ---

type resolveRequest struct {
	ResolutionToken string            `json:"resolution_token"`
	Resolution      aurora.Resolution `json:"resolution"`
}

func (h *handler) resolveTask(w http.ResponseWriter, r *http.Request) {
	var req resolveRequest
	if !readJSON(w, r, &req) {
		return
	}
	task, err := h.dist.Runtime.ResolveTask(r.PathValue("id"), req.ResolutionToken, req.Resolution)
	writeJSON(w, task, err)
}

// --- programs ---

func (h *handler) listPrograms(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, h.dist.Runtime.Programs(), nil)
}

func (h *handler) reloadPrograms(w http.ResponseWriter, r *http.Request) {
	artifacts, err := h.dist.ReloadPrograms(r.Context())
	writeJSON(w, artifacts, err)
}

func (h *handler) programRetention(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, h.dist.Retention(), nil)
}

// --- SSE ---

// sessionEvents streams one session: the runtime's snapshot event first, then
// live events, exactly the runtime Subscribe contract.
func (h *handler) sessionEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	initial, events, cancel, err := h.dist.Runtime.Subscribe(r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	defer cancel()

	startSSE(w)
	// The event name travels in the SSE event field; the data field carries
	// the payload itself, not a {type,data} envelope (the terminal is the
	// contract test here — it decodes payloads directly).
	writeSSE(w, initial.Type, "", initial.Data)
	flusher.Flush()

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-events:
			if !ok {
				return
			}
			writeSSE(w, event.Type, "", event.Data)
			flusher.Flush()
		}
	}
}

// firehose streams the whole tenant. Resume with Last-Event-ID (or ?after=):
// a cursor still inside the replay ring continues seamlessly; anything older
// gets a fresh `snapshot` event (current session summaries) before live
// frames — at-least-once, duplicates possible, gaps only ever explicit as a
// new snapshot.
func (h *handler) firehose(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	after := uint64(0)
	if raw := r.Header.Get("Last-Event-ID"); raw != "" {
		after, _ = strconv.ParseUint(raw, 10, 64)
	} else if raw := r.URL.Query().Get("after"); raw != "" {
		after, _ = strconv.ParseUint(raw, 10, 64)
	}
	replay, snapshot, live, cancel, err := h.dist.SubscribeFirehose(after)
	if err != nil {
		writeError(w, err)
		return
	}
	defer cancel()

	startSSE(w)
	if snapshot != nil {
		writeSSE(w, "snapshot", "", map[string]any{"sessions": snapshot})
	}
	for _, frame := range replay {
		writeSSE(w, frame.Type, strconv.FormatUint(frame.Seq, 10), frame)
	}
	flusher.Flush()

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case frame, ok := <-live:
			if !ok {
				// Disconnected for lag (or shutdown); the client re-syncs on
				// reconnect.
				return
			}
			writeSSE(w, frame.Type, strconv.FormatUint(frame.Seq, 10), frame)
			flusher.Flush()
		}
	}
}

func startSSE(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
}

func writeSSE(w http.ResponseWriter, event, id string, payload any) {
	data, err := json.Marshal(payload)
	if err != nil {
		return
	}
	if id != "" {
		_, _ = fmt.Fprintf(w, "id: %s\n", id)
	}
	_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, data)
}

// --- helpers ---

func readJSON(w http.ResponseWriter, r *http.Request, out any) bool {
	if err := json.NewDecoder(r.Body).Decode(out); err != nil {
		http.Error(w, fmt.Sprintf("invalid request body: %v", err), http.StatusBadRequest)
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, payload any, err error) {
	if err != nil {
		writeError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if encodeErr := json.NewEncoder(w).Encode(payload); encodeErr != nil {
		http.Error(w, encodeErr.Error(), http.StatusInternalServerError)
	}
}

func writeError(w http.ResponseWriter, err error) {
	http.Error(w, err.Error(), statusFor(err))
}

func statusFor(err error) int {
	switch {
	case errors.Is(err, aurora.ErrNotFound), errors.Is(err, aurora.ErrTaskNotFound):
		return http.StatusNotFound
	case errors.Is(err, aurora.ErrInvalid):
		return http.StatusBadRequest
	case errors.Is(err, aurora.ErrConflict), errors.Is(err, aurora.ErrTaskConflict):
		return http.StatusConflict
	case errors.Is(err, aurora.ErrTaskUnauthorized):
		return http.StatusUnauthorized
	case errors.Is(err, aurora.ErrTaskGone):
		return http.StatusGone
	default:
		return http.StatusInternalServerError
	}
}
