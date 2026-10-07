// Package dist assembles the Aurora distribution: the aurora-capcompute
// runtime compiled together with a fixed driver set (internet, scratch,
// filesystem, httptemplate, command, openaillm), concrete stores (in-memory or
// SQLite), and the runtime-adjacent services that must not live in terminals
// — the program directory kept in sync with the runtime by polling, and the
// static capability ceiling. One binary, one HTTP API in
// front (internal/api).
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
	"github.com/aurora-capcompute/aurora-dispatchers/openaillm"
	"github.com/aurora-capcompute/aurora-dispatchers/registry"

	"github.com/aurora-capcompute/aurora-dispatchers/command"
	"github.com/aurora-capcompute/aurora-dispatchers/filesystem"
	"github.com/aurora-capcompute/aurora-dispatchers/httptemplate"
	"github.com/aurora-capcompute/aurora-dispatchers/internet"
	"github.com/aurora-capcompute/aurora-dist/internal/dist/programs"
	"github.com/aurora-capcompute/aurora-dist/internal/dist/store/memory"
	"github.com/aurora-capcompute/aurora-dist/internal/dist/store/sqlite"
)

// Config wires one distribution instance.
type Config struct {
	// DataDir holds the SQLite database (<DataDir>/aurora.db). Empty runs on
	// in-memory stores — nothing survives a restart.
	DataDir string
	// ProgramsDir is scanned for *.wasm program artifacts. There is no default
	// program: every create-process request names the program it runs.
	ProgramsDir string
	// CapabilityCeiling lists every capability name this deployment may
	// grant; empty means unrestricted. CreateProcess refuses manifests
	// granting beyond it.
	CapabilityCeiling []string
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

	// ProgramReloadInterval is how often the programs directory is re-scanned
	// into the runtime (0 = 10s).
	ProgramReloadInterval time.Duration

	Logger *slog.Logger
}

// Dist is a running distribution: the runtime plus its compiled-in services.
type Dist struct {
	Runtime  aurora.Runtime
	Programs programs.Dir

	ceiling    *ceiling
	loopCancel context.CancelFunc
	closers    []io.Closer
	logger     *slog.Logger
}

// New assembles and starts a distribution: stores, driver registry, runtime
// (restoring persisted sessions), and the program reload loop.
func New(ctx context.Context, cfg Config) (*Dist, error) {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
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

	provider := newProvider([]registry.Registration{
		internet.Registration{},
		filesystem.Registration{},
		httptemplate.Registration{},
		command.Registration{},
		openaillm.Registration{},
	}, registry.Services{
		Secrets:  cfg.Secrets,
		AuditKey: cfg.AuditKey,
	}, spaces)

	dir := programs.Dir{Path: cfg.ProgramsDir}
	runtime, err := aurora.NewRuntime(ctx, aurora.Config{
		Dispatchers:            provider,
		Log:                    log,
		TaskSecret:             cfg.TaskSecret,
		MaxConcurrentProcesses: cfg.MaxConcurrentProcesses,
	})
	if err != nil {
		for _, closer := range closers {
			_ = closer.Close()
		}
		return nil, err
	}

	// The runtime boots with no programs: it reads no filesystem. Load the
	// directory into it before serving, so a distribution that cannot register
	// its programs fails to start rather than accepting work it cannot run.
	if _, err := dir.Reload(ctx, runtime); err != nil {
		_ = runtime.Close(ctx)
		for _, closer := range closers {
			_ = closer.Close()
		}
		return nil, err
	}

	loopCtx, loopCancel := context.WithCancel(context.Background())
	d := &Dist{
		Runtime:    runtime,
		Programs:   dir,
		ceiling:    newCeiling(cfg.CapabilityCeiling),
		loopCancel: loopCancel,
		closers:    closers,
		logger:     logger,
	}
	// The programs directory is re-scanned into the runtime on a ticker so
	// in-memory programs track the filesystem.
	d.startProgramReload(loopCtx, cfg.ProgramReloadInterval)
	// A host restart is not a process failure: re-drive everything the crash
	// left mid-flight so recovery needs no human. Parked processes (a timer, an
	// approval) resume on their own when the wait resolves; only interrupted
	// ones need the kick.
	d.resumeInterrupted()
	return d, nil
}

// resumeInterrupted re-drives every process a host failure left mid-flight.
// restore() marks processes that were running/queued/stopping at the crash as
// interrupted — an interruption is external (shutdown or a quantum deadline),
// never a program failure (which finishes as ProcessFailed) — so
// re-driving is safe: replay serves the committed journal prefix, and only a
// call whose completion was never journaled runs again.
func (d *Dist) resumeInterrupted() {
	for _, summary := range d.Runtime.ListSessions() {
		session, err := d.Runtime.GetSession(summary.ID)
		if err != nil {
			continue
		}
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

// CreateProcess starts a process on a session after the distribution's own
// gate: the capability ceiling. Manifest validation proper happens inside the
// runtime (ValidateManifest against the compiled driver set).
// deadline bounds each of the process's quanta; zero takes the runtime's host
// default, and the runtime clamps anything above its ceiling.
func (d *Dist) CreateProcess(sessionID, input string, manifest aurora.Manifest, deadline time.Duration) (aurora.ProcessSnapshot, error) {
	if err := d.ceiling.check(manifest); err != nil {
		return aurora.ProcessSnapshot{}, err
	}
	return d.Runtime.CreateProcess(sessionID, input, manifest, deadline)
}

// startProgramReload re-scans the programs directory into the runtime every
// interval so the in-memory program set converges to the filesystem. Reload adds
// and removes program by program — unchanged programs keep running — so an
// unchanged directory is a cheap no-op. With no directory configured there is
// nothing to track and the loop is not started.
func (d *Dist) startProgramReload(ctx context.Context, interval time.Duration) {
	if strings.TrimSpace(d.Programs.Path) == "" {
		return
	}
	if interval <= 0 {
		interval = 10 * time.Second
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if _, err := d.Programs.Reload(ctx, d.Runtime); err != nil {
					d.logger.Warn("program reload", "error", err)
				}
			}
		}
	}()
}

// Close shuts the distribution down: stops the program reload loop, closes the
// runtime (bounded by ctx), then the stores.
func (d *Dist) Close(ctx context.Context) error {
	if d.loopCancel != nil {
		d.loopCancel()
	}
	errs := []error{d.Runtime.Close(ctx)}
	for _, closer := range d.closers {
		errs = append(errs, closer.Close())
	}
	if ctx.Err() != nil {
		errs = append(errs, ctx.Err())
	}
	return errors.Join(errs...)
}
