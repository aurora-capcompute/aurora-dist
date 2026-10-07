package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"

	drivermem "github.com/aurora-capcompute/aurora-dispatchers/memory"
)

const scratchSchema = `
CREATE TABLE IF NOT EXISTS scratch_values (
	tenant_id  TEXT    NOT NULL,
	session_id TEXT    NOT NULL,
	key        TEXT    NOT NULL,
	value      BLOB    NOT NULL,
	labels     TEXT    NOT NULL DEFAULT '[]',
	version    INTEGER NOT NULL,
	PRIMARY KEY (tenant_id, session_id, key)
);
CREATE TABLE IF NOT EXISTS scratch_activities (
	tenant_id  TEXT    NOT NULL,
	session_id TEXT    NOT NULL,
	activity   TEXT    NOT NULL,
	version    INTEGER NOT NULL,
	PRIMARY KEY (tenant_id, session_id, activity)
);
`

// ScratchSpace is one session's durable core.scratch space. Every query is
// bound to the tenant and session, so the driver that holds it can reach no
// other session's keys.
//
// scratch_activities is the activity memory: one row per executed put intent,
// written in the same transaction as the value it records, so a re-driven put
// either finds its row or the write never happened. It is its own table, never
// rows in scratch_values, so a record cannot leak through Get or List.
func (s *Store) ScratchSpace(tenant, session string) drivermem.Store {
	return scratchSpace{db: s.db, tenant: tenant, session: session}
}

type scratchSpace struct {
	db      *sql.DB
	tenant  string
	session string
}

func (sp scratchSpace) Get(ctx context.Context, key string) (json.RawMessage, []string, int64, bool, error) {
	var (
		value   []byte
		rawLbls string
		version int64
	)
	err := sp.db.QueryRowContext(ctx,
		`SELECT value,labels,version FROM scratch_values WHERE tenant_id=? AND session_id=? AND key=?`,
		sp.tenant, sp.session, key).Scan(&value, &rawLbls, &version)
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

// Put writes key under the version expectation. With an activity key, the
// dedupe check, the write and the activity record share one transaction.
func (sp scratchSpace) Put(ctx context.Context, key string, value json.RawMessage, labels []string, expect int64, activity string) (int64, error) {
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

	if activity != "" {
		var recorded int64
		switch err := tx.QueryRowContext(ctx,
			`SELECT version FROM scratch_activities WHERE tenant_id=? AND session_id=? AND activity=?`,
			sp.tenant, sp.session, activity).Scan(&recorded); {
		case err == sql.ErrNoRows:
		case err != nil:
			return 0, err
		default:
			return recorded, nil // this intent already wrote; replay its outcome
		}
	}
	var current int64 // zero when absent
	switch err := tx.QueryRowContext(ctx,
		`SELECT version FROM scratch_values WHERE tenant_id=? AND session_id=? AND key=?`,
		sp.tenant, sp.session, key).Scan(&current); {
	case err == sql.ErrNoRows:
	case err != nil:
		return 0, err
	}
	if expect != drivermem.PutAny && expect != current {
		return 0, drivermem.ErrConflict
	}
	next := current + 1
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO scratch_values(tenant_id,session_id,key,value,labels,version) VALUES(?,?,?,?,?,?)
		 ON CONFLICT(tenant_id,session_id,key) DO UPDATE SET value=excluded.value, labels=excluded.labels, version=excluded.version`,
		sp.tenant, sp.session, key, []byte(value), string(rawLbls), next); err != nil {
		return 0, err
	}
	if activity != "" {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO scratch_activities(tenant_id,session_id,activity,version) VALUES(?,?,?,?)`,
			sp.tenant, sp.session, activity, next); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return next, nil
}

func (sp scratchSpace) Activity(ctx context.Context, activity string) (int64, bool, error) {
	var version int64
	err := sp.db.QueryRowContext(ctx,
		`SELECT version FROM scratch_activities WHERE tenant_id=? AND session_id=? AND activity=?`,
		sp.tenant, sp.session, activity).Scan(&version)
	if err == sql.ErrNoRows {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return version, true, nil
}

// List returns the keys at or under prefix — whole path segments, so "notes"
// lists "notes/a" but never "notes2" — sorted, each with its value's labels.
func (sp scratchSpace) List(ctx context.Context, prefix string) ([]drivermem.ListedKey, error) {
	rows, err := sp.db.QueryContext(ctx,
		`SELECT key,labels FROM scratch_values
		 WHERE tenant_id=? AND session_id=? AND (?='' OR key=? OR substr(key,1,length(?)+1)=?||'/')
		 ORDER BY key`,
		sp.tenant, sp.session, prefix, prefix, prefix, prefix)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var keys []drivermem.ListedKey
	for rows.Next() {
		var key, rawLbls string
		if err := rows.Scan(&key, &rawLbls); err != nil {
			return nil, err
		}
		var labels []string
		if err := json.Unmarshal([]byte(rawLbls), &labels); err != nil {
			return nil, err
		}
		keys = append(keys, drivermem.ListedKey{Key: key, Labels: labels})
	}
	return keys, rows.Err()
}
