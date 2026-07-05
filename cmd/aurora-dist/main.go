// aurora-dist is the Aurora distribution: one binary assembling the runtime
// with a compiled-in driver set and stores, exposed over one HTTP API.
//
//	aurora-dist -addr :8080 -data ./data -programs ./programs [-config dist.json]
//
// The task secret comes from AURORA_TASK_SECRET (or AURORA_TASK_SECRET_FILE);
// everything else from flags or the optional JSON config file (flags win).
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/aurora-capcompute/aurora-dispatchers/mcp"

	"github.com/aurora-capcompute/aurora-dist/internal/api"
	"github.com/aurora-capcompute/aurora-dist/internal/dist"
)

var version = "dev"

// fileConfig is the optional JSON config file. Flags override its fields.
type fileConfig struct {
	Addr     string `json:"addr,omitempty"`
	TenantID string `json:"tenant_id,omitempty"`
	DataDir  string `json:"data_dir,omitempty"`
	Programs struct {
		Dir     string `json:"dir,omitempty"`
		Default string `json:"default,omitempty"`
	} `json:"programs"`
	MCPServers            map[string]mcp.ServerConfig `json:"mcp_servers,omitempty"`
	CapabilityCeiling     []string                    `json:"capability_ceiling,omitempty"`
	InstanceID            string                      `json:"instance_id,omitempty"`
	MaxConcurrent         int                         `json:"max_concurrent_processes,omitempty"`
	MaxResident           int                         `json:"max_resident_processes,omitempty"`
	TimerReconcileSeconds int                         `json:"timer_reconcile_seconds,omitempty"`
	ProgramReloadSeconds  int                         `json:"program_reload_seconds,omitempty"`
}

func main() {
	if err := run(); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("aurora-dist stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		configPath  = flag.String("config", "", "path to a JSON config file")
		addr        = flag.String("addr", "", "listen address (default :8080)")
		dataDir     = flag.String("data", "", "data directory for the SQLite store (empty = in-memory)")
		programsDir = flag.String("programs", "", "directory of *.wasm program artifacts")
		defaultProg = flag.String("default-program", "", "default program id")
		tenantID    = flag.String("tenant", "", "tenant id (default \"local\")")
		showVersion = flag.Bool("version", false, "print version and exit")
	)
	flag.Parse()
	if *showVersion {
		fmt.Println(version)
		return nil
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	var cfg fileConfig
	if *configPath != "" {
		raw, err := os.ReadFile(*configPath)
		if err != nil {
			return fmt.Errorf("read config: %w", err)
		}
		if err := json.Unmarshal(raw, &cfg); err != nil {
			return fmt.Errorf("parse config %s: %w", *configPath, err)
		}
	}
	pick := func(flagValue, fileValue, fallback string) string {
		if strings.TrimSpace(flagValue) != "" {
			return flagValue
		}
		if strings.TrimSpace(fileValue) != "" {
			return fileValue
		}
		return fallback
	}

	taskSecret, err := secretFromEnv("AURORA_TASK_SECRET")
	if err != nil {
		return err
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if dir := pick(*dataDir, cfg.DataDir, ""); dir != "" {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return fmt.Errorf("create data directory: %w", err)
		}
	}
	d, err := dist.New(ctx, dist.Config{
		TenantID:               pick(*tenantID, cfg.TenantID, ""),
		DataDir:                pick(*dataDir, cfg.DataDir, ""),
		ProgramsDir:            pick(*programsDir, cfg.Programs.Dir, ""),
		DefaultProgram:         pick(*defaultProg, cfg.Programs.Default, ""),
		MCPServers:             cfg.MCPServers,
		CapabilityCeiling:      cfg.CapabilityCeiling,
		TaskSecret:             taskSecret,
		InstanceID:             cfg.InstanceID,
		MaxConcurrentProcesses: cfg.MaxConcurrent,
		MaxResidentProcesses:   cfg.MaxResident,
		TimerReconcileInterval: time.Duration(cfg.TimerReconcileSeconds) * time.Second,
		ProgramReloadInterval:  time.Duration(cfg.ProgramReloadSeconds) * time.Second,
		Logger:                 logger,
	})
	if err != nil {
		return fmt.Errorf("assemble distribution: %w", err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer closeCancel()
		if err := d.Close(closeCtx); err != nil {
			logger.Error("close distribution", "error", err)
		}
	}()

	server := &http.Server{
		Addr:    pick(*addr, cfg.Addr, ":8080"),
		Handler: api.Handler(d),
	}
	errs := make(chan error, 1)
	go func() {
		logger.Info("aurora-dist listening", "addr", server.Addr, "version", version)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errs <- err
		}
	}()

	select {
	case err := <-errs:
		return err
	case <-ctx.Done():
	}
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	_ = server.Shutdown(shutdownCtx)
	return nil
}

// secretFromEnv reads NAME, or the file NAME_FILE points at.
func secretFromEnv(name string) ([]byte, error) {
	if value := os.Getenv(name); value != "" {
		return []byte(value), nil
	}
	if path := os.Getenv(name + "_FILE"); path != "" {
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read %s_FILE: %w", name, err)
		}
		secret := strings.TrimSpace(string(raw))
		if secret == "" {
			return nil, fmt.Errorf("%s_FILE is empty", name)
		}
		return []byte(secret), nil
	}
	return nil, fmt.Errorf("%s (or %s_FILE) is required", name, name)
}
