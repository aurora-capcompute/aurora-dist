// Package sqlite is the distribution's durable store: an append-only event
// log (one ordered stream per session), a lease table for cross-instance
// coordination, and the tenant-memory KV behind core.memory — with the
// activity memory that makes the driver's intent→completion crash window
// exactly-once. The runtime folds the log into session/process/task
// projections; there is no per-entity row store.
package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"time"

	"github.com/aurora-capcompute/aurora-capcompute/aurora"
	drivermem "github.com/aurora-capcompute/aurora-dispatchers/memory"
	_ "github.com/mattn/go-sqlite3"
)

var (
	_ aurora.EventLog = (*Store)(nil)
	_ aurora.Leases   = (*Store)(nil)
	_ drivermem.Store = (*Store)(nil)
)

type Store struct {
	db *sql.DB
}

func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite3", path+"?_foreign_keys=on&_busy_timeout=5000&_journal_mode=WAL")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	store := &Store{db: db}
	if err := store.migrate(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) migrate(ctx context.Context) error {
	const schema = `
CREATE TABLE IF NOT EXISTS events (
	tenant_id   TEXT    NOT NULL,
	session_id  TEXT    NOT NULL,
	seq         INTEGER NOT NULL,
	kind        TEXT    NOT NULL,
	occurred_at TEXT    NOT NULL,
	process_id  TEXT    NOT NULL DEFAULT '',
	revision    INTEGER NOT NULL DEFAULT 0,
	data        BLOB,
	PRIMARY KEY (tenant_id, session_id, seq)
);
CREATE TABLE IF NOT EXISTS leases (
	key        TEXT PRIMARY KEY,
	holder     TEXT NOT NULL,
	expires_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS memory_values (
	tenant_id TEXT    NOT NULL,
	key       TEXT    NOT NULL,
	value     BLOB    NOT NULL,
	labels    TEXT    NOT NULL DEFAULT '[]',
	version   INTEGER NOT NULL,
	PRIMARY KEY (tenant_id, key)
);
CREATE TABLE IF NOT EXISTS memory_activities (
	tenant_id  TEXT    NOT NULL,
	activity   TEXT    NOT NULL,
	version    INTEGER NOT NULL,
	created_at TEXT    NOT NULL,
	PRIMARY KEY (tenant_id, activity)
);`
	// memory_activities is the memory driver's activity memory: one row per
	// executed put intent, written in the same transaction as the value it
	// records, so a re-driven put either finds its row or the write never
	// happened. Its own table — never rows in memory_values — so records can
	// never leak through Get/List. created_at is the future GC handle: an
	// intent older than the oldest journal that could re-drive it is dead
	// weight, prunable whenever a journal-lifecycle story lands.
	_, err := s.db.ExecContext(ctx, schema)
	return err
}

// Append atomically appends events to a session's stream, assigning each the
// next contiguous sequence, and returns the new head.
func (s *Store) Append(ctx context.Context, scope aurora.LogScope, events ...aurora.LogEvent) (uint64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()

	var head uint64
	if err := tx.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(seq), 0) FROM events WHERE tenant_id=? AND session_id=?`,
		scope.TenantID, scope.SessionID).Scan(&head); err != nil {
		return 0, err
	}
	for _, ev := range events {
		head++
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO events(tenant_id,session_id,seq,kind,occurred_at,process_id,revision,data)
			 VALUES(?,?,?,?,?,?,?,?)`,
			scope.TenantID, scope.SessionID, head, ev.Kind,
			ev.Time.UTC().Format(time.RFC3339Nano), ev.Proc, ev.Rev, []byte(ev.Data)); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return head, nil
}

// Read returns a stream's events with seq > after, in order.
func (s *Store) Read(ctx context.Context, scope aurora.LogScope, after uint64) ([]aurora.LogEvent, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT seq,kind,occurred_at,process_id,revision,data FROM events
		 WHERE tenant_id=? AND session_id=? AND seq>? ORDER BY seq`,
		scope.TenantID, scope.SessionID, after)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []aurora.LogEvent
	for rows.Next() {
		var (
			ev         aurora.LogEvent
			occurredAt string
			data       []byte
		)
		if err := rows.Scan(&ev.Seq, &ev.Kind, &occurredAt, &ev.Proc, &ev.Rev, &data); err != nil {
			return nil, err
		}
		if t, perr := time.Parse(time.RFC3339Nano, occurredAt); perr == nil {
			ev.Time = t
		}
		ev.Data = append(json.RawMessage(nil), data...)
		out = append(out, ev)
	}
	return out, rows.Err()
}

// Streams lists the session scopes that have events for a tenant.
func (s *Store) Streams(ctx context.Context, tenantID string) ([]aurora.LogScope, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT DISTINCT session_id FROM events WHERE tenant_id=? ORDER BY session_id`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []aurora.LogScope
	for rows.Next() {
		var sessionID string
		if err := rows.Scan(&sessionID); err != nil {
			return nil, err
		}
		out = append(out, aurora.LogScope{TenantID: tenantID, SessionID: sessionID})
	}
	return out, rows.Err()
}

// Acquire grants or renews a lease unless a different holder's unexpired lease
// exists.
func (s *Store) Acquire(ctx context.Context, tenantID, kind, resourceID, holder string, now time.Time, ttl time.Duration) (bool, error) {
	key := tenantID + "/" + kind + "/" + resourceID
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()

	var curHolder, curExpires string
	switch err := tx.QueryRowContext(ctx,
		`SELECT holder,expires_at FROM leases WHERE key=?`, key).Scan(&curHolder, &curExpires); {
	case err == sql.ErrNoRows:
	case err != nil:
		return false, err
	default:
		expires, _ := time.Parse(time.RFC3339Nano, curExpires)
		if curHolder != holder && now.Before(expires) {
			return false, nil
		}
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO leases(key,holder,expires_at) VALUES(?,?,?)
		 ON CONFLICT(key) DO UPDATE SET holder=excluded.holder, expires_at=excluded.expires_at`,
		key, holder, now.Add(ttl).UTC().Format(time.RFC3339Nano)); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

// Release drops a lease the holder owns.
func (s *Store) Release(ctx context.Context, tenantID, kind, resourceID, holder string) error {
	key := tenantID + "/" + kind + "/" + resourceID
	_, err := s.db.ExecContext(ctx, `DELETE FROM leases WHERE key=? AND holder=?`, key, holder)
	return err
}

// Get reads one tenant-memory key with its provenance labels and version.
func (s *Store) Get(ctx context.Context, tenant, key string) (json.RawMessage, []string, int64, bool, error) {
	var (
		value   []byte
		rawLbls string
		version int64
	)
	err := s.db.QueryRowContext(ctx,
		`SELECT value,labels,version FROM memory_values WHERE tenant_id=? AND key=?`,
		tenant, key).Scan(&value, &rawLbls, &version)
	if err == sql.ErrNoRows {
		return nil, nil, 0, false, nil
	}
	if err != nil {
		return nil, nil, 0, false, err
	}
	var labels []string
	if err := json.Unmarshal([]byte(rawLbls), &labels); err != nil {
		return nil, nil, 0, false, err
	}
	return append(json.RawMessage(nil), value...), labels, version, true, nil
}

// Put writes one tenant-memory key under the version expectation (PutAny /
// PutAbsent / exact version), returning the new version or ErrConflict. A
// non-empty activity key makes the write exactly-once: the dedupe check, the
// write, and the activity record ride one transaction, so there is no window
// in which the value is written but the intent unremembered — a re-driven put
// either replays its recorded version or the write never happened.
func (s *Store) Put(ctx context.Context, tenant, key string, value json.RawMessage, labels []string, expect int64, activity string) (int64, error) {
	if labels == nil {
		labels = []string{}
	}
	rawLbls, err := json.Marshal(labels)
	if err != nil {
		return 0, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()

	if activity != "" {
		var recorded int64
		switch err := tx.QueryRowContext(ctx,
			`SELECT version FROM memory_activities WHERE tenant_id=? AND activity=?`,
			tenant, activity).Scan(&recorded); {
		case err == sql.ErrNoRows:
		case err != nil:
			return 0, err
		default:
			return recorded, nil // this intent already wrote; replay its outcome
		}
	}
	var version int64
	exists := true
	switch err := tx.QueryRowContext(ctx,
		`SELECT version FROM memory_values WHERE tenant_id=? AND key=?`,
		tenant, key).Scan(&version); {
	case err == sql.ErrNoRows:
		exists = false
	case err != nil:
		return 0, err
	}
	switch {
	case expect == drivermem.PutAny:
	case expect == drivermem.PutAbsent && exists:
		return 0, drivermem.ErrConflict
	case expect > 0 && (!exists || version != expect):
		return 0, drivermem.ErrConflict
	}
	next := version + 1
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO memory_values(tenant_id,key,value,labels,version) VALUES(?,?,?,?,?)
		 ON CONFLICT(tenant_id,key) DO UPDATE SET value=excluded.value, labels=excluded.labels, version=excluded.version`,
		tenant, key, []byte(value), string(rawLbls), next); err != nil {
		return 0, err
	}
	if activity != "" {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO memory_activities(tenant_id,activity,version,created_at) VALUES(?,?,?,?)`,
			tenant, activity, next, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return next, nil
}

// Activity reports whether a put under this activity key already executed for
// the tenant, and the version it recorded.
func (s *Store) Activity(ctx context.Context, tenant, activity string) (int64, bool, error) {
	var version int64
	err := s.db.QueryRowContext(ctx,
		`SELECT version FROM memory_activities WHERE tenant_id=? AND activity=?`,
		tenant, activity).Scan(&version)
	if err == sql.ErrNoRows {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return version, true, nil
}

// List returns a tenant's memory keys under a prefix, sorted.
func (s *Store) List(ctx context.Context, tenant, prefix string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT key FROM memory_values WHERE tenant_id=? ORDER BY key`, tenant)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, err
		}
		// Segment-aware, tolerant of a trailing slash: "notes" and "notes/" both
		// list the "notes" subtree ("notes/a", …) but never the sibling "notes2".
		base := strings.TrimSuffix(prefix, "/")
		if prefix == "" || key == prefix || strings.HasPrefix(key, base+"/") {
			keys = append(keys, key)
		}
	}
	return keys, rows.Err()
}
