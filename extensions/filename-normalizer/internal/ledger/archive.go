package ledger

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/jobs"
)

// Archival states. They are stored in source_archivals and used as the values
// of a closed-set metric label.
const (
	ArchivalPending  = "pending"
	ArchivalArchived = "archived"
	ArchivalRemoved  = "removed"
	ArchivalAbsent   = "absent"
	ArchivalRefused  = "refused"
)

// What happens to a delivered original.
const (
	// ArchiveMove moves it into a directory inside its source root.
	ArchiveMove = "move"
	// ArchiveRemove removes it from its source root: the delivered copy is
	// the same bytes, verified before it was published.
	ArchiveRemove = "remove"
)

// Archival is one delivered job whose original is due to be dealt with.
type Archival struct {
	Job Job
	// Action is ArchiveMove or ArchiveRemove, as the job was accepted with.
	Action string
	// ArchiveDir is where a move goes; empty for a removal.
	ArchiveDir string
	Attempts   int
}

// ArchivalsDue returns delivered jobs from one source root whose originals are
// still to be moved or removed, oldest request first.
//
// Only DELIVERED jobs. An original whose job was held or is uncertain stays in
// the drop folder: it is what a person resolving that job starts from, and
// moving it would put the one document that did not reach Paperless out of
// sight.
//
// Only jobs registered from THIS root. The archive directory is resolved
// against the root the caller is configured with, and a job accepted from a
// different root is not the caller's to move.
func (l *Ledger) ArchivalsDue(ctx context.Context, root string, limit int) ([]Archival, error) {
	rows, err := l.primary.Query(ctx, `
		SELECT `+prefixedJobColumns("j")+`, a.archive_dir, a.attempts, a.action
		  FROM source_archivals a
		  JOIN jobs j ON j.job_id = a.job_id
		 WHERE a.state = 'pending'
		   AND a.next_attempt_at <= now()
		   AND j.state = 'delivered'
		   AND j.source_root = $1
		 ORDER BY a.requested_at
		 LIMIT $2`, root, limit)
	if err != nil {
		return nil, fmt.Errorf("list due archivals: %w", err)
	}
	defer rows.Close()

	var out []Archival
	for rows.Next() {
		var a Archival
		job, err := scanJob(rows, &a.ArchiveDir, &a.Attempts, &a.Action)
		if err != nil {
			return nil, err
		}
		a.Job = job
		out = append(out, a)
	}
	return out, rows.Err()
}

// ErrArchivalSettled reports that an archival already reached an outcome. It
// is not a failure: another pass got there first, and what it recorded stands.
var ErrArchivalSettled = errors.New("the archival already has an outcome")

// SettleArchival records the outcome of moving a job's original.
//
// It only settles a PENDING row. A second pass that raced this one, or a
// process that recorded the move and died before it could forget about it,
// must not overwrite what was recorded first.
func (l *Ledger) SettleArchival(ctx context.Context, jobID, state, archivedName, category string) error {
	switch state {
	case ArchivalArchived, ArchivalRemoved, ArchivalAbsent, ArchivalRefused:
	default:
		return fmt.Errorf("refusing to settle an archival as %q", state)
	}
	return l.tx(ctx, func(tx pgx.Tx) error {
		ct, err := tx.Exec(ctx, `
			UPDATE source_archivals
			   SET state = $2,
			       archived_name = NULLIF($3, ''),
			       category = NULLIF($4, ''),
			       attempts = attempts + 1,
			       settled_at = now(),
			       updated_at = now()
			 WHERE job_id = $1 AND state = 'pending'`,
			jobID, state, archivedName, category)
		if err != nil {
			return err
		}
		if ct.RowsAffected() == 0 {
			return ErrArchivalSettled
		}
		detail := map[string]any{"archive_state": state}
		var cat *string
		if category != "" {
			cat = ptr(category)
		}
		return l.appendEvent(ctx, tx, eventInput{
			JobID:     jobID,
			EventType: jobs.EventSourceArchived,
			Category:  cat,
			Detail:    detail,
		})
	})
}

// DeferArchival records a failed attempt that may succeed later, and when the
// next one is due.
//
// A storage fault on the NAS -- the share briefly away, a permission not yet
// granted -- is not an outcome. The row stays pending, so nothing about the
// original is decided on the strength of a failure that says nothing about it.
func (l *Ledger) DeferArchival(ctx context.Context, jobID, category string, wait time.Duration) error {
	_, err := l.primary.Exec(ctx, `
		UPDATE source_archivals
		   SET attempts = attempts + 1,
		       category = NULLIF($2, ''),
		       next_attempt_at = now() + $3::interval,
		       updated_at = now()
		 WHERE job_id = $1 AND state = 'pending'`,
		jobID, category, wait.String())
	if err != nil {
		return fmt.Errorf("defer an archival: %w", err)
	}
	return nil
}

// ArchivalFor returns a job's archival row: its state, and where the original
// went when it was moved. ErrNotFound means no archival was requested.
func (l *Ledger) ArchivalFor(ctx context.Context, jobID string) (state, archivedName, category string, err error) {
	var name, cat *string
	qerr := l.primary.QueryRow(ctx, `
		SELECT state, archived_name, category FROM source_archivals WHERE job_id = $1`,
		jobID).Scan(&state, &name, &cat)
	if errors.Is(qerr, pgx.ErrNoRows) {
		return "", "", "", ErrNotFound
	}
	if qerr != nil {
		return "", "", "", fmt.Errorf("read an archival: %w", qerr)
	}
	if name != nil {
		archivedName = *name
	}
	if cat != nil {
		category = *cat
	}
	return state, archivedName, category, nil
}

// ArchivalsWaiting counts delivered jobs whose originals have not been moved or
// removed yet. A number that keeps growing means the drop folder is filling
// with documents that were delivered and never tidied away.
func (l *Ledger) ArchivalsWaiting(ctx context.Context, root string) (int, error) {
	var n int
	err := l.primary.QueryRow(ctx, `
		SELECT count(*)
		  FROM source_archivals a
		  JOIN jobs j ON j.job_id = a.job_id
		 WHERE a.state = 'pending' AND j.state = 'delivered' AND j.source_root = $1`,
		root).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("count waiting archivals: %w", err)
	}
	return n, nil
}
