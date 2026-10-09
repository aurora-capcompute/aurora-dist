package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/aurora-capcompute/aurora-dispatchers/scratch"
)

const scratchSchema = `
CREATE TABLE IF NOT EXISTS scratch_values (
	session_id TEXT    NOT NULL,
	key        TEXT    NOT NULL,
	value      BLOB    NOT NULL,
	labels     TEXT    NOT NULL DEFAULT '[]',
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

func (sp scratchSpace) Get(ctx context.Context, key string) (json.RawMessage, []string, bool, error) {
	var (
		value   []byte
		rawLbls string
	)
	err := sp.db.QueryRowContext(ctx,
		`SELECT value,labels FROM scratch_values WHERE session_id=? AND key=?`,
		sp.session, key).Scan(&value, &rawLbls)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil, false, nil
	}
	if err != nil {
		return nil, nil, false, err
	}
	// Put wrote the labels as a JSON list.
	var labels []string
	_ = json.Unmarshal([]byte(rawLbls), &labels)
	return value, labels, true, nil
}

func (sp scratchSpace) Put(ctx context.Context, key string, value json.RawMessage, labels []string) error {
	rawLbls, _ := json.Marshal(labels)
	_, err := sp.db.ExecContext(ctx,
		`INSERT INTO scratch_values(session_id,key,value,labels) VALUES(?,?,?,?)
		 ON CONFLICT(session_id,key) DO UPDATE SET value=excluded.value, labels=excluded.labels`,
		sp.session, key, []byte(value), string(rawLbls))
	return err
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
		_ = json.Unmarshal([]byte(rawLbls), &labels)
		keys = append(keys, scratch.ListedKey{Key: key, Labels: labels})
	}
	return keys, rows.Err()
}
