// Package api serves the distribution's HTTP API — the single way into the
// runtime, versioned /v1 from birth (the resolution_token and session_id
// renames were the cautionary tales). It is a thin projection of the runtime
// surface plus the distribution's own gate: the capability ceiling enforced at
// process creation.
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

	"github.com/aurora-capcompute/aurora-capcompute/aurora"

	"github.com/aurora-capcompute/aurora-dist/internal/dist"
)

// Handler serves the /v1 API over an assembled distribution.
func Handler(d *dist.Dist) http.Handler {
	h := &handler{dist: d}
	mux := http.NewServeMux()

	// Sessions. GET returns the complete session log — session metadata,
	// history, and every process with its full state, delegation links,
	// journal across all revisions, and tasks. Every narrower view (the
	// current journal, one revision, the call graph, a task list) is a
	// client-side grouping of that one payload.
	mux.HandleFunc("GET /v1/sessions", h.listSessions)
	mux.HandleFunc("POST /v1/sessions", h.createSession)
	mux.HandleFunc("GET /v1/sessions/{id}", h.getSession)
	mux.HandleFunc("POST /v1/sessions/{id}/rename", h.renameSession)
	mux.HandleFunc("POST /v1/sessions/{id}/processes", h.createProcess)

	// Processes. A single-process snapshot is kept for cheap status polling;
	// everything richer lives in the session log.
	mux.HandleFunc("GET /v1/processes/{id}", h.getProcess)
	mux.HandleFunc("POST /v1/processes/{id}/stop", h.stopProcess)
	mux.HandleFunc("POST /v1/processes/{id}/retry", h.retryProcess)

	// Tasks.
	mux.HandleFunc("POST /v1/tasks/{id}/resolve", h.resolveTask)

	// Programs. Read-only: the loaded artifact set — the set itself is
	// reconciled from the programs directory by the distribution's poller.
	mux.HandleFunc("GET /v1/programs", h.listPrograms)

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

func (h *handler) listPrograms(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, h.dist.Runtime.Programs(), nil)
}

type createSessionRequest struct {
	// Name is the session's explicit handle (unique per tenant; empty = unnamed,
	// the id is then the handle).
	Name string            `json:"name,omitempty"`
	Tags map[string]string `json:"tags,omitempty"`
}

func (h *handler) createSession(w http.ResponseWriter, r *http.Request) {
	var req createSessionRequest
	if r.ContentLength != 0 && !readJSON(w, r, &req) {
		return
	}
	snapshot, err := h.dist.CreateSession(req.Name, req.Tags)
	if err != nil {
		writeError(w, err)
		return
	}
	// Return the created session in the same shape a GET would, so a client
	// has one session representation to decode.
	log, err := h.dist.SessionLog(snapshot.ID)
	writeJSON(w, log, err)
}

type renameSessionRequest struct {
	Name string `json:"name"`
}

func (h *handler) renameSession(w http.ResponseWriter, r *http.Request) {
	var req renameSessionRequest
	if !readJSON(w, r, &req) {
		return
	}
	if _, err := h.dist.RenameSession(r.PathValue("id"), req.Name); err != nil {
		writeError(w, err)
		return
	}
	log, err := h.dist.SessionLog(r.PathValue("id"))
	writeJSON(w, log, err)
}

// getSession returns the complete session log: the one comprehensive read.
func (h *handler) getSession(w http.ResponseWriter, r *http.Request) {
	log, err := h.dist.SessionLog(r.PathValue("id"))
	writeJSON(w, log, err)
}

// --- processes ---

type createProcessRequest struct {
	// Input is the process's input — exactly what the program's declared input
	// schema accepts (string-first: plain text, or a JSON document).
	Input string `json:"input"`
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
	snapshot, err := h.dist.CreateProcess(r.PathValue("id"), req.Input, manifest)
	writeJSON(w, snapshot, err)
}

// getProcess is the cheap single-process status poll (no journal or tasks —
// those live in the session log).
func (h *handler) getProcess(w http.ResponseWriter, r *http.Request) {
	snapshot, err := h.dist.Runtime.GetProcess(r.PathValue("id"))
	writeJSON(w, snapshot, err)
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

// --- helpers ---

func readJSON(w http.ResponseWriter, r *http.Request, out any) bool {
	if err := json.NewDecoder(r.Body).Decode(out); err != nil {
		writeError(w, fmt.Errorf("%w: invalid request body: %v", aurora.ErrInvalid, err))
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
		writeError(w, encodeErr)
	}
}

// errorBody is the one error shape the API emits: a human message plus a stable
// machine-readable code, so a client branches on the class rather than parsing
// prose.
type errorBody struct {
	Error string `json:"error"`
	Code  string `json:"code"`
}

func writeError(w http.ResponseWriter, err error) {
	status, code := classify(err)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(errorBody{Error: err.Error(), Code: code})
}

// classify maps a runtime error to its HTTP status and stable code in one place,
// so the two can never drift.
func classify(err error) (int, string) {
	switch {
	case errors.Is(err, aurora.ErrNotFound), errors.Is(err, aurora.ErrTaskNotFound):
		return http.StatusNotFound, "not_found"
	case errors.Is(err, aurora.ErrInvalid):
		return http.StatusBadRequest, "invalid_args"
	case errors.Is(err, aurora.ErrConflict), errors.Is(err, aurora.ErrTaskConflict):
		return http.StatusConflict, "conflict"
	case errors.Is(err, aurora.ErrTaskUnauthorized):
		return http.StatusUnauthorized, "unauthorized"
	case errors.Is(err, aurora.ErrTaskGone):
		return http.StatusGone, "gone"
	default:
		return http.StatusInternalServerError, "internal"
	}
}
