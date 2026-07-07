// Package dist assembles the Aurora distribution: the aurora-capcompute
// runtime compiled together with a fixed driver set (builtin router,
// internet, memory, openaillm), concrete stores (in-memory or
// SQLite), and the runtime-adjacent services that must not live in terminals
// — timer firing, the program directory kept in sync with the runtime by
// polling, and the static capability ceiling. One binary, one HTTP API in
// front (internal/api).
package dist

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aurora-capcompute/aurora-capcompute/aurora"
	drivermem "github.com/aurora-capcompute/aurora-dispatchers/memory"
	"github.com/aurora-capcompute/aurora-dispatchers/openaillm"
	"github.com/aurora-capcompute/aurora-dispatchers/registry"

	"github.com/aurora-capcompute/aurora-dist/internal/dist/programs"
	"github.com/aurora-capcompute/aurora-dist/internal/dist/store/memory"
	"github.com/aurora-capcompute/aurora-dist/internal/dist/store/sqlite"
	"github.com/aurora-capcompute/aurora-dist/internal/dist/timers"
)

// Config wires one distribution instance.
type Config struct {
	// TenantID scopes every session this instance serves (single-tenant
	// posture; empty uses the runtime default).
	TenantID string
	// DataDir holds the SQLite database (<DataDir>/aurora.db). Empty runs on
	// in-memory stores — nothing survives a restart.
	DataDir string
	// ProgramsDir is scanned for *.wasm program artifacts; DefaultProgram
	// optionally names the default program id.
	ProgramsDir    string
	DefaultProgram string
	// CapabilityCeiling lists every capability name this deployment may
	// grant; empty means unrestricted. CreateProcess refuses manifests
	// granting beyond it.
	CapabilityCeiling []string
	// TaskSecret derives task resolution tokens. Required.
	TaskSecret []byte
	// InstanceID identifies this instance for lease fencing.
	InstanceID string

	MaxConcurrentProcesses int
	MaxResidentProcesses   int

	// TimerReconcileInterval is how often the timer service reconciles its
	// armed timers against runtime state (0 = 1s).
	TimerReconcileInterval time.Duration
	// ProgramReloadInterval is how often the programs directory is re-scanned
	// into the runtime (0 = 10s).
	ProgramReloadInterval time.Duration

	Logger *slog.Logger
}

// Dist is a running distribution: the runtime plus its compiled-in services.
type Dist struct {
	Runtime  aurora.Runtime
	Timers   *timers.Service
	Programs programs.Dir

	ceiling    *ceiling
	loopCancel context.CancelFunc
	closers    []io.Closer
	logger     *slog.Logger
}

// New assembles and starts a distribution: stores, driver registry, runtime
// (restoring persisted sessions), and the timer + program reconcile loops.
func New(ctx context.Context, cfg Config) (*Dist, error) {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	var (
		log     aurora.EventLog
		leases  aurora.Leases
		kv      drivermem.Store
		closers []io.Closer
	)
	if strings.TrimSpace(cfg.DataDir) == "" {
		log = memory.NewEventLog()
		leases = memory.NewLeases()
		kv = memory.NewKV()
	} else {
		store, err := sqlite.Open(filepath.Join(cfg.DataDir, "aurora.db"))
		if err != nil {
			return nil, fmt.Errorf("open store: %w", err)
		}
		log, leases, kv = store, store, store
		closers = append(closers, store)
	}

	tenant := strings.TrimSpace(cfg.TenantID)
	if tenant == "" {
		tenant = aurora.DefaultTenantID
	}
	provider := newProvider([]registry.Registration{
		registry.InternetRegistration{},
		registry.MemoryRegistration{},
		openaillm.Registration{},
	}, registry.Services{
		Tenant:      tenant,
		MemoryStore: kv,
	})

	dir := programs.Dir{Path: cfg.ProgramsDir, Default: cfg.DefaultProgram}
	// A restart must reclaim the leases of the processes it was running: the
	// process lease is keyed by holder id, so the restarted instance needs the
	// *same* id to renew immediately rather than wait out the dead instance's
	// lease. Persist a stable id beside the durable store; an explicit config
	// id wins, and in-memory runs (no durable processes) keep a fresh id.
	instanceID := strings.TrimSpace(cfg.InstanceID)
	if instanceID == "" && strings.TrimSpace(cfg.DataDir) != "" {
		id, idErr := stableInstanceID(cfg.DataDir)
		if idErr != nil {
			logger.Warn("stable instance id unavailable; a fast restart may wait out in-flight leases", "error", idErr)
		} else {
			instanceID = id
		}
	}
	runtime, err := aurora.NewRuntime(ctx, aurora.Config{
		Programs:               dir,
		Dispatchers:            provider,
		Log:                    log,
		Leases:                 leases,
		ProcessTable:           memory.NewProcessTable[string, aurora.ProcessContext](),
		TenantID:               tenant,
		TaskSecret:             cfg.TaskSecret,
		InstanceID:             instanceID,
		MaxConcurrentProcesses: cfg.MaxConcurrentProcesses,
		MaxResidentProcesses:   cfg.MaxResidentProcesses,
	})
	if err != nil {
		for _, closer := range closers {
			_ = closer.Close()
		}
		return nil, err
	}

	loopCtx, loopCancel := context.WithCancel(context.Background())
	d := &Dist{
		Runtime:    runtime,
		Timers:     timers.New(runtime, logger),
		Programs:   dir,
		ceiling:    newCeiling(cfg.CapabilityCeiling),
		loopCancel: loopCancel,
		closers:    closers,
		logger:     logger,
	}
	// Timers reconcile their armed set against runtime state on a ticker (and
	// once now, for boot recovery); the programs directory is re-scanned into
	// the runtime on its own ticker so in-memory programs track the filesystem.
	d.Timers.Start(loopCtx, cfg.TimerReconcileInterval)
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
// interrupted — an interruption is external (shutdown, a scheduling or lease
// conflict), never a program failure (which finishes as ProcessFailed) — so
// re-driving is safe and idempotent: replay serves the committed journal
// prefix and re-executes only from the last open savepoint. A delegated child
// whose parent is also interrupted is left to its parent, whose resumed quantum
// re-drives it via replay; kicking it directly would double-drive the tree. So
// only the topmost interrupted node of each tree is retried.
func (d *Dist) resumeInterrupted() {
	for _, summary := range d.Runtime.ListSessions() {
		session, err := d.Runtime.GetSession(summary.ID)
		if err != nil {
			continue
		}
		graph, err := d.Runtime.SessionGraph(summary.ID)
		if err != nil {
			continue
		}
		parent := make(map[string]string, len(graph.Processes))
		for _, gp := range graph.Processes {
			parent[gp.ProcessID] = gp.ParentProcessID
		}
		interrupted := make(map[string]bool)
		for _, process := range session.Processes {
			if process.Status == aurora.ProcessInterrupted {
				interrupted[process.ID] = true
			}
		}
		for id := range interrupted {
			if interrupted[parent[id]] {
				continue // re-driven inside its interrupted parent's quantum
			}
			if _, err := d.Runtime.Retry(id, aurora.RetryResume); err != nil {
				d.logger.Warn("resume interrupted process at boot", "process_id", id, "error", err)
			}
		}
	}
}

// stableInstanceID returns a lease-holder id that survives restarts: it reads
// <dataDir>/instance_id if present, else mints one and persists it. Stability
// is what lets a restarted instance renew (rather than wait out) the process
// leases its previous life still holds.
func stableInstanceID(dataDir string) (string, error) {
	path := filepath.Join(dataDir, "instance_id")
	if raw, err := os.ReadFile(path); err == nil {
		if id := strings.TrimSpace(string(raw)); id != "" {
			return id, nil
		}
	}
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", err
	}
	id := "inst_" + hex.EncodeToString(buf[:])
	if err := os.WriteFile(path, []byte(id), 0o600); err != nil {
		return "", err
	}
	return id, nil
}

// CreateSession creates a session.
func (d *Dist) CreateSession(tags map[string]string) (aurora.SessionSnapshot, error) {
	return d.Runtime.CreateSession(tags)
}

// CreateProcess starts a process on a session after the distribution's own
// gate: the capability ceiling. Manifest validation proper happens inside the
// runtime (ValidateManifest against the compiled driver set).
func (d *Dist) CreateProcess(sessionID, input string, manifest aurora.Manifest) (aurora.ProcessSnapshot, error) {
	if err := d.ceiling.check(manifest); err != nil {
		return aurora.ProcessSnapshot{}, err
	}
	return d.Runtime.CreateProcess(sessionID, input, manifest)
}

// startProgramReload re-scans the programs directory into the runtime every
// interval so the in-memory program set converges to the filesystem. The scan
// is digest-diffed by SetPrograms — unchanged programs keep running — so an
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

// Close shuts the distribution down: stops the reconcile loops and timers,
// closes the runtime (bounded by ctx), then the stores.
func (d *Dist) Close(ctx context.Context) error {
	if d.loopCancel != nil {
		d.loopCancel()
	}
	d.Timers.StopAll()
	errs := []error{d.Runtime.Close(ctx)}
	for _, closer := range d.closers {
		errs = append(errs, closer.Close())
	}
	if ctx.Err() != nil {
		errs = append(errs, ctx.Err())
	}
	return errors.Join(errs...)
}
