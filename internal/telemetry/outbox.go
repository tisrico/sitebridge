// Package telemetry implements durable store-and-forward delivery of site
// telemetry to a cloud ingest endpoint over an unreliable network.
package telemetry

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	_ "modernc.org/sqlite" // pure-Go SQLite: no cgo, simple cross-compilation
)

// Record is one buffered telemetry record.
type Record struct {
	Seq  int64           `json:"seq"`
	TS   time.Time       `json:"ts"`
	Data json.RawMessage `json:"data"`
}

// Outbox is a durable FIFO of telemetry records in SQLite.
//
// Every record is committed to disk before it is eligible for upload, and is
// deleted only after the cloud acknowledges it, so delivery is at-least-once
// across crashes, reboots and outages. Sequence numbers come from an
// AUTOINCREMENT key and are never reused, even after the table drains, so the
// receiver can de-duplicate on (stream, seq). The stream ID is generated once
// when the database is created; if the database is lost or replaced the new
// stream ID tells the receiver that numbering restarted.
type Outbox struct {
	db         *sql.DB
	maxBacklog int
	streamID   string

	mu      sync.Mutex
	count   int
	dropped int64
}

// OpenOutbox opens (or creates) the outbox at path. synchronous is "FULL"
// (survives power loss) or "NORMAL" (survives process crashes; faster, may
// lose the last transactions on power loss).
func OpenOutbox(path, synchronous string, maxBacklog int) (*Outbox, error) {
	if synchronous != "FULL" && synchronous != "NORMAL" {
		return nil, fmt.Errorf("invalid synchronous mode %q", synchronous)
	}
	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=synchronous(%s)&_pragma=busy_timeout(5000)", path, synchronous)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	// One connection: SQLite serialises writers anyway, and this keeps the
	// pragmas on the single connection that is used.
	db.SetMaxOpenConns(1)

	stmts := []string{
		`CREATE TABLE IF NOT EXISTS outbox (
			seq  INTEGER PRIMARY KEY AUTOINCREMENT,
			ts   INTEGER NOT NULL,
			data BLOB    NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			db.Close()
			return nil, fmt.Errorf("init outbox: %w", err)
		}
	}

	o := &Outbox{db: db, maxBacklog: maxBacklog}
	if err := o.loadStreamID(); err != nil {
		db.Close()
		return nil, err
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM outbox`).Scan(&o.count); err != nil {
		db.Close()
		return nil, err
	}
	return o, nil
}

func (o *Outbox) loadStreamID() error {
	err := o.db.QueryRow(`SELECT value FROM meta WHERE key='stream_id'`).Scan(&o.streamID)
	if err == nil {
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return err
	}
	o.streamID = hex.EncodeToString(b)
	_, err = o.db.Exec(`INSERT INTO meta(key, value) VALUES('stream_id', ?)`, o.streamID)
	return err
}

// StreamID identifies this outbox's sequence space.
func (o *Outbox) StreamID() string { return o.streamID }

// Append durably stores a record and returns its sequence number. If the
// backlog is full the oldest records are discarded: on a long outage the most
// recent data is the most useful, and the device must not run out of storage.
// Discards are counted and reported, and show up at the receiver as a gap.
func (o *Outbox) Append(ctx context.Context, ts time.Time, data []byte) (int64, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	tx, err := o.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	res, err := tx.ExecContext(ctx, `INSERT INTO outbox(ts, data) VALUES(?, ?)`, ts.UnixMilli(), data)
	if err != nil {
		return 0, err
	}
	seq, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	var trimmed int64
	if over := o.count + 1 - o.maxBacklog; o.maxBacklog > 0 && over > 0 {
		r, err := tx.ExecContext(ctx,
			`DELETE FROM outbox WHERE seq IN (SELECT seq FROM outbox ORDER BY seq LIMIT ?)`, over)
		if err != nil {
			return 0, err
		}
		trimmed, _ = r.RowsAffected()
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	o.count += 1 - int(trimmed)
	o.dropped += trimmed
	return seq, nil
}

// Peek returns up to limit of the oldest records without removing them.
func (o *Outbox) Peek(ctx context.Context, limit int) ([]Record, error) {
	rows, err := o.db.QueryContext(ctx, `SELECT seq, ts, data FROM outbox ORDER BY seq LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Record
	for rows.Next() {
		var r Record
		var ts int64
		var data []byte
		if err := rows.Scan(&r.Seq, &ts, &data); err != nil {
			return nil, err
		}
		r.TS = time.UnixMilli(ts).UTC()
		r.Data = data
		out = append(out, r)
	}
	return out, rows.Err()
}

// Ack removes all records up to and including seq.
func (o *Outbox) Ack(ctx context.Context, seq int64) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	res, err := o.db.ExecContext(ctx, `DELETE FROM outbox WHERE seq <= ?`, seq)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	o.count -= int(n)
	return nil
}

// Len returns the number of records waiting for upload.
func (o *Outbox) Len() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.count
}

// Dropped returns how many records were discarded because the backlog was full.
func (o *Outbox) Dropped() int64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.dropped
}

// IntegrityCheck runs SQLite's integrity check.
func (o *Outbox) IntegrityCheck() error {
	var res string
	if err := o.db.QueryRow(`PRAGMA integrity_check`).Scan(&res); err != nil {
		return err
	}
	if res != "ok" {
		return fmt.Errorf("integrity check: %s", res)
	}
	return nil
}

// Close closes the database.
func (o *Outbox) Close() error { return o.db.Close() }
