// Package dist assembles the Aurora distribution: the aurora-capcompute
// runtime compiled together with a fixed driver set (builtin router,
// internet, MCP, memory, timer, openaillm), concrete stores (in-memory or
// SQLite), and the runtime-adjacent services that must not live in terminals
// — timer firing, the program registry with its retention query, the tenant
// event firehose, and the static capability ceiling. One binary, one HTTP+SSE
// API in front (internal/api).
package dist

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"strings"

	"github.com/aurora-capcompute/aurora-capcompute/aurora"
	"github.com/aurora-capcompute/aurora-dispatchers/mcp"
	drivermem "github.com/aurora-capcompute/aurora-dispatchers/memory"
	"github.com/aurora-capcompute/aurora-dispatchers/openaillm"
	"github.com/aurora-capcompute/aurora-dispatchers/registry"

	"github.com/aurora-capcompute/aurora-dist/internal/programs"
	"github.com/aurora-capcompute/aurora-dist/internal/store/memory"
	"github.com/aurora-capcompute/aurora-dist/internal/store/sqlite"
	"github.com/aurora-capcompute/aurora-dist/internal/timers"
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
	// MCPServers registers stdio MCP servers by id for core.mcp grants.
	MCPServers map[string]mcp.ServerConfig
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
	// FirehoseRing bounds the replay window of the tenant event firehose
	// (frames; 0 = 8192).
	FirehoseRing int

	Logger *slog.Logger
}

// Dist is a running distribution: the runtime plus its compiled-in services.
type Dist struct {
	Runtime  aurora.Runtime
	Timers   *timers.Service
	Programs programs.Dir

	firehose *firehose
	ceiling  *ceiling
	closers  []io.Closer
	logger   *slog.Logger
}

// New assembles and starts a distribution: stores, driver registry, runtime
// (restoring persisted sessions), firehose watches, and timer recovery.
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
	servers := make(map[string]mcp.ServerConfig, len(cfg.MCPServers))
	for id, server := range cfg.MCPServers {
		if strings.TrimSpace(server.ID) == "" {
			server.ID = id
		}
		servers[id] = server
	}
	provider := newProvider([]registry.Registration{
		registry.InternetRegistration{},
		registry.MCPRegistration{},
		registry.AuroraLogRegistration{},
		registry.TimerRegistration{},
		registry.MemoryRegistration{},
		openaillm.Registration{},
	}, registry.Services{
		MCPServers:  servers,
		Tenant:      tenant,
		MemoryStore: kv,
	})

	dir := programs.Dir{Path: cfg.ProgramsDir, Default: cfg.DefaultProgram}
	runtime, err := aurora.NewRuntime(ctx, aurora.Config{
		Programs:               dir,
		Dispatchers:            provider,
		Log:                    log,
		Leases:                 leases,
		ProcessTable:           memory.NewProcessTable[string, aurora.ProcessContext](),
		TenantID:               tenant,
		TaskSecret:             cfg.TaskSecret,
		InstanceID:             cfg.InstanceID,
		MaxConcurrentProcesses: cfg.MaxConcurrentProcesses,
		MaxResidentProcesses:   cfg.MaxResidentProcesses,
	})
	if err != nil {
		for _, closer := range closers {
			_ = closer.Close()
		}
		return nil, err
	}

	d := &Dist{
		Runtime:  runtime,
		Timers:   timers.New(runtime, logger),
		Programs: dir,
		firehose: newFirehose(runtime, cfg.FirehoseRing),
		ceiling:  newCeiling(cfg.CapabilityCeiling),
		closers:  closers,
		logger:   logger,
	}
	// The timer service observes the merged event stream as an internal tap
	// (never disconnected for lag) and re-arms persisted timers at boot.
	d.firehose.tap = func(frame Frame) { d.Timers.Observe(frame.Type, frame.Data) }
	if err := d.firehose.start(); err != nil {
		_ = d.Close(context.Background())
		return nil, fmt.Errorf("start firehose: %w", err)
	}
	d.Timers.Recover(runtime)
	return d, nil
}

// CreateSession creates a session and announces it on the firehose.
func (d *Dist) CreateSession(tags map[string]string) (aurora.SessionSnapshot, error) {
	snapshot, err := d.Runtime.CreateSession(tags)
	if err != nil {
		return snapshot, err
	}
	if err := d.firehose.sessionCreated(snapshot); err != nil {
		d.logger.Warn("watch created session", "session_id", snapshot.ID, "error", err)
	}
	return snapshot, nil
}

// CreateProcess starts a process on a session after the distribution's own
// gate: the capability ceiling. Manifest validation proper happens inside the
// runtime (ValidateManifest against the compiled driver set).
func (d *Dist) CreateProcess(sessionID, message string, manifest aurora.Manifest) (aurora.ProcessSnapshot, error) {
	if err := d.ceiling.check(manifest); err != nil {
		return aurora.ProcessSnapshot{}, err
	}
	return d.Runtime.CreateProcess(sessionID, message, manifest)
}

// SubscribeFirehose attaches a tenant-wide event subscriber; see
// firehose.subscribe for the resume/re-sync contract.
func (d *Dist) SubscribeFirehose(after uint64) ([]Frame, []aurora.SessionSummary, <-chan Frame, func(), error) {
	return d.firehose.subscribe(after)
}

// ReloadPrograms re-scans the programs directory into the runtime.
func (d *Dist) ReloadPrograms(ctx context.Context) ([]aurora.ProgramArtifact, error) {
	return d.Programs.Reload(ctx, d.Runtime)
}

// Retention answers the digest retention query over current process state.
func (d *Dist) Retention() []programs.Reference {
	return programs.Retention(d.Runtime)
}

// Close shuts the distribution down: timers, firehose, runtime (bounded by
// ctx), then the stores.
func (d *Dist) Close(ctx context.Context) error {
	d.Timers.StopAll()
	d.firehose.close()
	errs := []error{d.Runtime.Close(ctx)}
	for _, closer := range d.closers {
		errs = append(errs, closer.Close())
	}
	if ctx.Err() != nil {
		errs = append(errs, ctx.Err())
	}
	return errors.Join(errs...)
}
