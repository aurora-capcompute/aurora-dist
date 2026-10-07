// Package sqlite is the distribution's durable store: an append-only event log
// (one ordered stream per session) and each session's core.scratch space. The runtime folds the
// log into session/process/task projections; there is no per-entity row store.
package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"github.com/aurora-capcompute/aurora-capcompute/aurora"
	_ "github.com/mattn/go-sqlite3"
)

var _ aurora.EventLog = (*Store)(nil)

type Store struct {
	db *sql.DB
}

func Open(path string) (*Store, error) {
	// synchronous=FULL: WAL's default (NORMAL) keeps a commit across a process
	// crash but may lose the last commits on an OS crash or power loss. A lost
	// intent record would let an effect run again with no trace of the first
	// run, so every commit is fsynced.
	db, err := sql.Open("sqlite3", path+"?_foreign_keys=on&_busy_timeout=5000&_journal_mode=WAL&_synchronous=FULL")
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
	session_id  TEXT    NOT NULL,
	seq         INTEGER NOT NULL,
	kind        TEXT    NOT NULL,
	occurred_at TEXT    NOT NULL,
	process_id  TEXT    NOT NULL DEFAULT '',
	revision    INTEGER NOT NULL DEFAULT 0,
	data        BLOB,
	PRIMARY KEY (session_id, seq)
);
`
	_, err := s.db.ExecContext(ctx, schema+scratchSchema)
	return err
}

// Append atomically appends events to a session's stream, assigning each the
// next contiguous sequence, and returns the new head.
func (s *Store) Append(ctx context.Context, sessionID string, events ...aurora.LogEvent) (uint64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()

	var head uint64
	if err := tx.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(seq), 0) FROM events WHERE session_id=?`,
		sessionID).Scan(&head); err != nil {
		return 0, err
	}
	for _, ev := range events {
		head++
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO events(session_id,seq,kind,occurred_at,process_id,revision,data)
			 VALUES(?,?,?,?,?,?,?)`,
			sessionID, head, ev.Kind,
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
func (s *Store) Read(ctx context.Context, sessionID string, after uint64) ([]aurora.LogEvent, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT seq,kind,occurred_at,process_id,revision,data FROM events
		 WHERE session_id=? AND seq>? ORDER BY seq`,
		sessionID, after)
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

// Sessions lists the sessions that have events.
func (s *Store) Sessions(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT DISTINCT session_id FROM events ORDER BY session_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var sessionID string
		if err := rows.Scan(&sessionID); err != nil {
			return nil, err
		}
		out = append(out, sessionID)
	}
	return out, rows.Err()
}
