package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/aurora-capcompute/capcompute/sys/replay/tape/journaled"
)

// marshalVerbatim encodes without HTML escaping so stored records carry the
// guest's syscall args and result bytes verbatim; json.Marshal would rewrite
// <, >, and & inside raw messages and a restored journal would diverge
// against its own re-executing guest.
func marshalVerbatim(payload any) ([]byte, error) {
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(payload); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

// Journal returns a durable journaled.Journal view over this store, keyed by
// an opaque journal id (typically a run PID). Records and the writer header
// are stored verbatim, so the hash chain the kernel's tape computes survives
// storage and `journaled.Verify` can audit a journal long after its run — the
// durable audit path for assemblies that wire kernel journals straight into
// SQLite rather than deriving them from an event log.
func (s *Store) Journal(id string) journaled.Journal {
	return &journalView{db: s.db, id: id}
}

// VerifyJournal walks a stored journal's structure and hash chain.
func (s *Store) VerifyJournal(id string) error {
	return journaled.Verify(s.Journal(id))
}

type journalView struct {
	db *sql.DB
	id string
}

func (j *journalView) Header() (journaled.Header, bool, error) {
	var raw []byte
	err := j.db.QueryRowContext(context.Background(),
		`SELECT header FROM journal_headers WHERE journal_id=?`, j.id).Scan(&raw)
	if err == sql.ErrNoRows {
		return journaled.Header{}, false, nil
	}
	if err != nil {
		return journaled.Header{}, false, err
	}
	var header journaled.Header
	if err := json.Unmarshal(raw, &header); err != nil {
		return journaled.Header{}, false, fmt.Errorf("decode journal header: %w", err)
	}
	return header, true, nil
}

func (j *journalView) SetHeader(header journaled.Header) error {
	raw, err := marshalVerbatim(header)
	if err != nil {
		return err
	}
	_, err = j.db.ExecContext(context.Background(),
		`INSERT INTO journal_headers(journal_id, header) VALUES(?,?)
		 ON CONFLICT(journal_id) DO UPDATE SET header=excluded.header`,
		j.id, raw)
	return err
}

func (j *journalView) Load(index int) (journaled.Record, error) {
	var raw []byte
	err := j.db.QueryRowContext(context.Background(),
		`SELECT record FROM journal_records WHERE journal_id=? AND position=?`,
		j.id, index).Scan(&raw)
	if err == sql.ErrNoRows {
		return journaled.Record{}, fmt.Errorf("journal record not found at index %d", index)
	}
	if err != nil {
		return journaled.Record{}, err
	}
	var record journaled.Record
	if err := json.Unmarshal(raw, &record); err != nil {
		return journaled.Record{}, fmt.Errorf("decode journal record %d: %w", index, err)
	}
	return record, nil
}

// Append persists the record at index Length(), atomically: the position
// check and the insert share one transaction so concurrent writers cannot
// interleave.
func (j *journalView) Append(record journaled.Record) error {
	raw, err := marshalVerbatim(record)
	if err != nil {
		return err
	}
	ctx := context.Background()
	tx, err := j.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	var length int
	if err := tx.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM journal_records WHERE journal_id=?`, j.id).Scan(&length); err != nil {
		return err
	}
	if record.Position != length {
		return fmt.Errorf("invalid journal position %d (want %d)", record.Position, length)
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO journal_records(journal_id, position, record) VALUES(?,?,?)`,
		j.id, record.Position, raw); err != nil {
		return err
	}
	return tx.Commit()
}

func (j *journalView) Length() int {
	var length int
	if err := j.db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM journal_records WHERE journal_id=?`, j.id).Scan(&length); err != nil {
		return 0
	}
	return length
}
