package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"

	"github.com/aurora-capcompute/aurora-dispatchers/scratch"
)

const scratchSchema = `
CREATE TABLE IF NOT EXISTS scratch_values (
	session_id TEXT    NOT NULL,
	key        TEXT    NOT NULL,
	value      BLOB    NOT NULL,
	labels     TEXT    NOT NULL DEFAULT '[]',
	version    INTEGER NOT NULL,
	PRIMARY KEY (session_id, key)
);
`

// ScratchSpace is one session's durable core.scratch space. Every query is
// bound to the session, so the driver that holds it can reach no other
// session's keys.
func (s *Store) ScratchSpace(session string) scratch.Store {
	return scratchSpace{db: s.db, session: session}
}

type scratchSpace struct {
	db      *sql.DB
	session string
}

func (sp scratchSpace) Get(ctx context.Context, key string) (json.RawMessage, []string, int64, bool, error) {
	var (
		value   []byte
		rawLbls string
		version int64
	)
	err := sp.db.QueryRowContext(ctx,
		`SELECT value,labels,version FROM scratch_values WHERE session_id=? AND key=?`,
		sp.session, key).Scan(&value, &rawLbls, &version)
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
	return value, labels, version, true, nil
}

// Put writes key under the version expectation; the version check and the
// write share one transaction.
func (sp scratchSpace) Put(ctx context.Context, key string, value json.RawMessage, labels []string, expect int64) (int64, error) {
	if labels == nil {
		labels = []string{}
	}
	rawLbls, err := json.Marshal(labels)
	if err != nil {
		return 0, err
	}
	tx, err := sp.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()

	var current int64 // zero when absent
	switch err := tx.QueryRowContext(ctx,
		`SELECT version FROM scratch_values WHERE session_id=? AND key=?`,
		sp.session, key).Scan(&current); {
	case err == sql.ErrNoRows:
	case err != nil:
		return 0, err
	}
	if expect != scratch.PutAny && expect != current {
		return 0, scratch.ErrConflict
	}
	next := current + 1
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO scratch_values(session_id,key,value,labels,version) VALUES(?,?,?,?,?)
		 ON CONFLICT(session_id,key) DO UPDATE SET value=excluded.value, labels=excluded.labels, version=excluded.version`,
		sp.session, key, []byte(value), string(rawLbls), next); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return next, nil
}

// List returns the keys at or under prefix — whole path segments, so "notes"
// lists "notes/a" but never "notes2" — sorted, each with its value's labels.
func (sp scratchSpace) List(ctx context.Context, prefix string) ([]scratch.ListedKey, error) {
	rows, err := sp.db.QueryContext(ctx,
		`SELECT key,labels FROM scratch_values
		 WHERE session_id=? AND (?='' OR key=? OR substr(key,1,length(?)+1)=?||'/')
		 ORDER BY key`,
		sp.session, prefix, prefix, prefix, prefix)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var keys []scratch.ListedKey
	for rows.Next() {
		var key, rawLbls string
		if err := rows.Scan(&key, &rawLbls); err != nil {
			return nil, err
		}
		var labels []string
		if err := json.Unmarshal([]byte(rawLbls), &labels); err != nil {
			return nil, err
		}
		keys = append(keys, scratch.ListedKey{Key: key, Labels: labels})
	}
	return keys, rows.Err()
}
