// Package dist assembles the Aurora distribution: the aurora-capcompute runtime
// with a fixed driver set (internet, scratch, httptemplate, command, openaillm),
// a store (in memory, or SQLite), and the programs directory loaded at boot.
// The api package serves it over HTTP.
package dist

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"time"

	"github.com/aurora-capcompute/aurora-capcompute/aurora"
	"github.com/aurora-capcompute/aurora-dispatchers/command"
	"github.com/aurora-capcompute/aurora-dispatchers/httptemplate"
	"github.com/aurora-capcompute/aurora-dispatchers/internet"
	"github.com/aurora-capcompute/aurora-dispatchers/openaillm"
	"github.com/aurora-capcompute/aurora-dispatchers/registry"
	"github.com/aurora-capcompute/aurora-dist/internal/dist/programs"
	"github.com/aurora-capcompute/aurora-dist/internal/dist/store/memory"
	"github.com/aurora-capcompute/aurora-dist/internal/dist/store/sqlite"
)

// Config wires one distribution instance.
type Config struct {
	// DataDir holds the SQLite database (<DataDir>/aurora.db). Empty runs on
	// in-memory stores — nothing survives a restart.
	DataDir string
	// ProgramsDir holds the *.wasm program artifacts, loaded once at boot. There
	// is no default program: every create-process request names the program it
	// runs.
	ProgramsDir string
	// TaskSecret derives task resolution tokens. Required.
	TaskSecret []byte
	// Secrets resolves manifest secret references (an injected Authorization
	// token, an api_key) to their host-held values. Nil means no references may
	// be used — a grant that references one fails to build, never at call time.
	Secrets registry.SecretResolver
	// AuditKey keys the credential fingerprints recorded when a secret is
	// injected into a request (empty is allowed: a stable but unkeyed
	// fingerprint). It is never the credential itself.
	AuditKey []byte

	MaxConcurrentProcesses int
	// QuantumDeadline bounds one quantum, including its driver calls (0 = the
	// runtime's default).
	QuantumDeadline time.Duration

	Logger *slog.Logger
}

// Dist is a running distribution: the runtime plus its compiled-in services.
type Dist struct {
	Runtime aurora.Runtime

	closers []io.Closer
	logger  *slog.Logger
}

// New assembles and starts a distribution: stores, driver registry, runtime
// (restoring persisted sessions), and its programs.
func New(ctx context.Context, cfg Config) (*Dist, error) {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	// The runtime reads no filesystem: the programs are read here and compiled
	// by the runtime at boot, so a distribution that cannot load its programs
	// fails to start rather than accepting work it cannot run.
	listed, err := programs.Load(cfg.ProgramsDir)
	if err != nil {
		return nil, err
	}

	var (
		log     aurora.EventLog
		spaces  scratchSpaces
		closers []io.Closer
	)
	if strings.TrimSpace(cfg.DataDir) == "" {
		log = memory.NewEventLog()
		spaces = memory.NewScratchSpaces()
	} else {
		store, err := sqlite.Open(filepath.Join(cfg.DataDir, "aurora.db"))
		if err != nil {
			return nil, fmt.Errorf("open store: %w", err)
		}
		log, spaces = store, store
		closers = append(closers, store)
	}

	provider := &provider{
		registrations: []registry.Registration{
			internet.Registration{},
			httptemplate.Registration{},
			command.Registration{},
			openaillm.Registration{},
		},
		services: registry.Services{Secrets: cfg.Secrets, AuditKey: cfg.AuditKey},
		scratch:  spaces,
	}

	runtime, err := aurora.NewRuntime(ctx, aurora.Config{
		Dispatchers:            provider,
		Programs:               listed,
		Log:                    log,
		TaskSecret:             cfg.TaskSecret,
		MaxConcurrentProcesses: cfg.MaxConcurrentProcesses,
		QuantumDeadline:        cfg.QuantumDeadline,
	})
	if err != nil {
		for _, closer := range closers {
			_ = closer.Close()
		}
		return nil, err
	}

	d := &Dist{
		Runtime: runtime,
		closers: closers,
		logger:  logger,
	}
	// A host restart is not a process failure: re-drive everything the crash
	// left mid-flight so recovery needs no human. Parked processes (a timer, an
	// approval) resume on their own when the wait resolves; only interrupted
	// ones need the kick.
	d.resumeInterrupted()
	return d, nil
}

// resumeInterrupted re-drives every process a host failure left mid-flight.
// Restore marks processes that were running, queued or stopping at the crash as
// interrupted — an interruption is external (shutdown or a quantum deadline),
// never a program failure (which finishes as ProcessFailed) — so re-driving is
// safe: replay serves the committed journal prefix, and only a call whose
// completion was never journaled runs again.
func (d *Dist) resumeInterrupted() {
	for _, summary := range d.Runtime.ListSessions() {
		session, _ := d.Runtime.GetSession(summary.ID)
		for _, process := range session.Processes {
			if process.Status != aurora.ProcessInterrupted {
				continue
			}
			if _, err := d.Runtime.Retry(process.ID, aurora.RetryResume); err != nil {
				d.logger.Warn("resume interrupted process at boot", "process_id", process.ID, "error", err)
			}
		}
	}
}

// Close shuts the distribution down: the runtime (bounded by ctx), then the
// stores.
func (d *Dist) Close(ctx context.Context) error {
	errs := []error{d.Runtime.Close(ctx)}
	for _, closer := range d.closers {
		errs = append(errs, closer.Close())
	}
	return errors.Join(errs...)
}
