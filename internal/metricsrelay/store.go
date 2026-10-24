// The ledger's sqlite representation: the relay_entries table in the
// shared metrics database — the ring's replacement (Q7, shared-metrics-db
// design). One row per recorded provider request, the json-tagged Entry
// fields as columns, At indexed for the age prune and the ascending
// walks. The golens-metrics embed opens its own connections to the same
// file (its tables are black-box); this package owns only its namespaced
// table and touches nothing else in the database.
package metricsrelay

import (
	"database/sql"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	// The pure-Go sqlite driver the golens-metrics embed already requires —
	// no new dependency, one driver serving both owners' connections.
	_ "modernc.org/sqlite"
)

// relaySchema is the ledger's namespaced slice of the shared database.
// Costs stay out of the rows: CostUSD is the reserved zero it has always
// been (the package comment) — the column only keeps a row's round-trip
// faithful to its Entry.
const relaySchema = `
CREATE TABLE IF NOT EXISTS relay_entries (
    at INTEGER NOT NULL,
    worker TEXT NOT NULL,
    queue TEXT NOT NULL,
    alias TEXT NOT NULL,
    wire TEXT NOT NULL,
    task TEXT NOT NULL,
    role TEXT NOT NULL,
    round TEXT NOT NULL,
    model TEXT NOT NULL,
    input_tokens INTEGER NOT NULL,
    output_tokens INTEGER NOT NULL,
    cache_read_tokens INTEGER NOT NULL,
    cache_write_tokens INTEGER NOT NULL,
    cost_usd REAL NOT NULL,
    duration_ms INTEGER NOT NULL,
    status INTEGER NOT NULL,
    source TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_relay_entries_at ON relay_entries(at);
CREATE INDEX IF NOT EXISTS idx_relay_entries_worker ON relay_entries(worker, at);
`

// relayCols is the SELECT/INSERT column list, in scan and bind order.
const relayCols = `at, worker, queue, alias, wire, task, role, round, model,
	input_tokens, output_tokens, cache_read_tokens, cache_write_tokens,
	cost_usd, duration_ms, status, source`

// StoreDSN appends the connection pragmas the shared file needs: WAL so
// the co-owners' readers never block on the ledger's writes, and a busy
// timeout so a colliding writer waits out the other's commit instead of
// failing. journal_mode persists in the file — every connection to it
// operates WAL. Every writer on the file takes its connection through
// this, the module's storage one included: it splices Storage.Path raw
// into sql.Open, sqlite's default busy timeout is zero, and a flush that
// collides with a ledger insert would otherwise fail immediately and
// drop that roll-up window for good. The one exception is
// DashboardConfigDB, which keeps a plain path — the module appends its
// own pragma suffix there, and a pre-DSN'd string would corrupt it.
func StoreDSN(path string) string {
	return path + "?_pragma=busy_timeout%3d5000&_pragma=journal_mode%3dWAL"
}

// Store is the sqlite-backed ledger. Safe for concurrent use; the daemon
// process is its only writer and reader.
type Store struct {
	db *sql.DB
	// maxAge bounds a row's life; 0 (or less) keeps everything forever.
	maxAge time.Duration
	// lastPrune holds the hour bucket of the last prune pass, so Append
	// runs the DELETE at most once per hour however many requests land.
	lastPrune atomic.Int64
}

// OpenStore opens the shared database at path, ensures the ledger schema,
// and runs the first age prune. The caller owns the path (MkdirAll'd) and
// closes the store with the daemon.
func OpenStore(path string, maxAge time.Duration) (*Store, error) {
	db, err := sql.Open("sqlite", StoreDSN(path))
	if err != nil {
		return nil, fmt.Errorf("metricsrelay: open ledger sqlite: %w", err)
	}
	if _, err := db.Exec(relaySchema); err != nil {
		db.Close()
		return nil, fmt.Errorf("metricsrelay: init ledger schema: %w", err)
	}
	s := &Store{db: db, maxAge: maxAge}
	if maxAge > 0 {
		now := time.Now()
		s.prune(now)
		s.lastPrune.Store(now.Unix() / int64(time.Hour/time.Second))
	}
	return s, nil
}

// Close releases the database connection.
func (s *Store) Close() error { return s.db.Close() }

// Append inserts one entry. A single INSERT is its own transaction — the
// sqlite analogue of the ring's append. Failures are logged, never fatal:
// the ledger is observability and must not red the request it records
// (the task log's best-effort posture). The hourly prune rides along.
func (s *Store) Append(e Entry) {
	_, err := s.db.Exec(`INSERT INTO relay_entries (`+relayCols+`) VALUES
		(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		e.At.UnixNano(), e.Worker, e.Queue, e.Alias, e.Wire, e.Task, e.Role,
		e.Round, e.Model, e.InputTokens, e.OutputTokens, e.CacheReadTokens,
		e.CacheWriteTokens, e.CostUSD, e.DurationMS, e.Status, e.Source)
	if err != nil {
		log.Printf("metricsrelay: ledger insert: %v", err)
	}
	s.maybePrune(time.Now())
}

// maybePrune runs the age prune at most once per hour of appends: the
// boot pass (OpenStore) plus one DELETE an hour is ample for a 45-day
// horizon, and the DELETE is the one statement that rewrites the table.
func (s *Store) maybePrune(now time.Time) {
	if s.maxAge <= 0 {
		return
	}
	bucket := now.Unix() / int64(time.Hour/time.Second)
	if s.lastPrune.Swap(bucket) == bucket {
		return
	}
	s.prune(now)
}

func (s *Store) prune(now time.Time) {
	_, err := s.db.Exec(`DELETE FROM relay_entries WHERE at < ?`, now.Add(-s.maxAge).UnixNano())
	if err != nil {
		log.Printf("metricsrelay: ledger prune: %v", err)
	}
}

// Len reports the ledger's row count. A failed count reads as zero and is
// logged — same best-effort posture as Append.
func (s *Store) Len() int {
	var n int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM relay_entries`).Scan(&n); err != nil {
		log.Printf("metricsrelay: ledger count: %v", err)
		return 0
	}
	return n
}

// Ascending returns every entry, oldest first — the folds' full-window
// read (the ascending walk the dashboard's bookkeeping depends on).
func (s *Store) Ascending() []Entry {
	rows, err := s.db.Query(`SELECT ` + relayCols + ` FROM relay_entries ORDER BY at ASC`)
	if err != nil {
		log.Printf("metricsrelay: ledger read: %v", err)
		return nil
	}
	defer rows.Close()
	var out []Entry
	for rows.Next() {
		e, err := scanEntry(rows)
		if err != nil {
			log.Printf("metricsrelay: ledger scan: %v", err)
			return nil
		}
		out = append(out, e)
	}
	// The iteration's own error surfaces only here — without the check a
	// mid-walk disconnect reads as a complete table and silently truncates
	// the folds' window.
	if err := rows.Err(); err != nil {
		log.Printf("metricsrelay: ledger read: %v", err)
		return nil
	}
	return out
}

// Query returns the newest entries first, one page's worth: page counts
// from 1, pageSize bounds it (below 1 means the 50 default — the
// request-side page cap lives at the Ledger, the interface every external
// request routes through), worker filters to one worker's entries when
// non-empty. total is the
// filtered count across all pages; a page past the last one — an
// overflowed page number included, whose offset would leave int's range
// in the multiply below — is empty.
func (s *Store) Query(worker string, page, pageSize int) (entries []Entry, total, size int) {
	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 50
	}
	size = pageSize
	where := ` WHERE (? = '' OR worker = ?)`
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM relay_entries`+where, worker, worker).Scan(&total); err != nil {
		log.Printf("metricsrelay: ledger count: %v", err)
		return nil, 0, size
	}
	// int64 arithmetic: total + pageSize - 1 would wrap for a page_size
	// near MaxInt, serving an empty page for an in-range one.
	if last := (int64(total) + int64(pageSize) - 1) / int64(pageSize); int64(page) > last {
		return nil, total, size
	}
	rows, err := s.db.Query(`SELECT `+relayCols+` FROM relay_entries`+where+
		` ORDER BY at DESC LIMIT ? OFFSET ?`,
		worker, worker, pageSize, (page-1)*pageSize)
	if err != nil {
		log.Printf("metricsrelay: ledger read: %v", err)
		return nil, total, size
	}
	defer rows.Close()
	for rows.Next() {
		e, err := scanEntry(rows)
		if err != nil {
			log.Printf("metricsrelay: ledger scan: %v", err)
			return nil, total, size
		}
		entries = append(entries, e)
	}
	if err := rows.Err(); err != nil {
		log.Printf("metricsrelay: ledger read: %v", err)
		return nil, total, size
	}
	return entries, total, size
}

// scanEntry reads one row. At carries no zone of its own in sqlite —
// time.Unix reconstitutes local time, and every consumer orders or
// formats rather than comparing zones.
func scanEntry(rows *sql.Rows) (Entry, error) {
	var e Entry
	var at int64
	if err := rows.Scan(&at, &e.Worker, &e.Queue, &e.Alias, &e.Wire, &e.Task,
		&e.Role, &e.Round, &e.Model, &e.InputTokens, &e.OutputTokens,
		&e.CacheReadTokens, &e.CacheWriteTokens, &e.CostUSD, &e.DurationMS,
		&e.Status, &e.Source); err != nil {
		return Entry{}, err
	}
	e.At = time.Unix(0, at)
	return e, nil
}
