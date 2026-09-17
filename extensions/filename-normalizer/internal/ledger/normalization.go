package ledger

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/jobs"
)

// Reservation is an exclusive claim on one destination name.
type Reservation struct {
	DestinationRoot string
	ReservationKey  string
	ReservedName    string
	JobID           string
	Sequence        int
	CreatedAt       time.Time
	BlockedAt       *time.Time
}

// Receipt is the durable evidence that a document was delivered.
type Receipt struct {
	JobID            string
	DestinationRoot  string
	DeliveredName    string
	SizeBytes        int64
	Fingerprint      []byte
	Attempt          int
	DeliveredAt      time.Time
	AbsentObservedAt *time.Time
}

// ErrReservedByAnother reports that a destination name belongs to a different
// job. It is not a failure: the caller advances to the next candidate.
var ErrReservedByAnother = errors.New("destination name reserved by another job")

// ReserveName claims one destination name for a job, atomically.
//
// Allocation is decided by the primary key on (destination_root,
// reservation_key): concurrent workers race on a single INSERT and the
// database picks exactly one winner. There is no read-then-write window, which
// is what makes two submissions normalizing to the same name safe.
//
// A retry of the same job re-reserving the same name succeeds and returns the
// existing row, so a job's destination is stable across attempts.
func (l *Ledger) ReserveName(ctx context.Context, jobID, root, key, name string, sequence int) (Reservation, error) {
	var r Reservation
	err := l.tx(ctx, func(tx pgx.Tx) error {
		row := tx.QueryRow(ctx, `
			INSERT INTO name_reservations
				(destination_root, reservation_key, reserved_name, job_id, sequence)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (destination_root, reservation_key) DO UPDATE
				-- A no-op update so the existing row is returned and can be
				-- inspected, rather than the statement returning nothing and
				-- forcing a second round trip that could race.
				SET reserved_name = name_reservations.reserved_name
			RETURNING destination_root, reservation_key, reserved_name, job_id,
			          sequence, created_at, blocked_at`,
			root, key, name, jobID, sequence)
		return row.Scan(&r.DestinationRoot, &r.ReservationKey, &r.ReservedName,
			&r.JobID, &r.Sequence, &r.CreatedAt, &r.BlockedAt)
	})
	if err != nil {
		return Reservation{}, fmt.Errorf("reserve destination name: %w", err)
	}
	if r.JobID != jobID {
		return r, ErrReservedByAnother
	}
	if r.BlockedAt != nil {
		// This job already tried this name and found it occupied.
		return r, ErrReservedByAnother
	}
	return r, nil
}

// BlockReservation records that a reserved name turned out to be occupied by a
// file this job did not publish.
//
// The row is kept rather than deleted. Deleting it would let the same job
// retry the same occupied name forever, and would also free the name for a
// different submission while the conflicting file is still there.
func (l *Ledger) BlockReservation(ctx context.Context, jobID, root, key string) error {
	return l.tx(ctx, func(tx pgx.Tx) error {
		ct, err := tx.Exec(ctx, `
			UPDATE name_reservations
			   SET blocked_at = now()
			 WHERE destination_root = $1 AND reservation_key = $2 AND job_id = $3`,
			root, key, jobID)
		if err != nil {
			return err
		}
		if ct.RowsAffected() == 0 {
			return fmt.Errorf("no reservation to block for job %s", jobID)
		}
		return l.appendEvent(ctx, tx, eventInput{
			JobID:     jobID,
			EventType: jobs.EventReservationBlocked,
			Category:  ptr(string(jobs.CategoryDestinationConflict)),
		})
	})
}

// ActiveReservation returns a job's unblocked reservation, if it has one.
func (l *Ledger) ActiveReservation(ctx context.Context, jobID, root string) (Reservation, error) {
	var r Reservation
	row := l.primary.QueryRow(ctx, `
		SELECT destination_root, reservation_key, reserved_name, job_id,
		       sequence, created_at, blocked_at
		  FROM name_reservations
		 WHERE job_id = $1 AND destination_root = $2 AND blocked_at IS NULL`,
		jobID, root)
	err := row.Scan(&r.DestinationRoot, &r.ReservationKey, &r.ReservedName,
		&r.JobID, &r.Sequence, &r.CreatedAt, &r.BlockedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Reservation{}, ErrNotFound
	}
	if err != nil {
		return Reservation{}, fmt.Errorf("read reservation: %w", err)
	}
	return r, nil
}

// RecordNormalized stores the computed name and moves the job to processing.
func (l *Ledger) RecordNormalized(ctx context.Context, jobID, normalized string, attempt int) error {
	return l.tx(ctx, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
			UPDATE jobs SET normalized_name = $2, updated_at = now() WHERE job_id = $1`,
			jobID, normalized)
		if err != nil {
			return err
		}
		return l.appendEvent(ctx, tx, eventInput{
			JobID:     jobID,
			EventType: jobs.EventNormalized,
			Attempt:   ptr(attempt),
		})
	})
}

// RecordPublishIntent marks a job as publishing and stamps the attempt time.
//
// It is committed BEFORE the destination link is attempted. That ordering is
// the whole point: if the process dies during publication, the ledger already
// says a publication may have happened, and recovery can distinguish that from
// a job that never got that far.
func (l *Ledger) RecordPublishIntent(ctx context.Context, jobID, reserved string, attempt int) error {
	return l.tx(ctx, func(tx pgx.Tx) error {
		ct, err := tx.Exec(ctx, `
			UPDATE jobs
			   SET state = 'publishing',
			       reserved_name = $2,
			       publish_attempted_at = now(),
			       updated_at = now()
			 WHERE job_id = $1 AND state IN ('processing', 'publishing')`,
			jobID, reserved)
		if err != nil {
			return err
		}
		if ct.RowsAffected() == 0 {
			return fmt.Errorf("job %s is not in a publishable state", jobID)
		}
		return l.appendEvent(ctx, tx, eventInput{
			JobID:     jobID,
			EventType: jobs.EventPublishAttempted,
			ToState:   ptr(string(jobs.StatePublishing)),
			Attempt:   ptr(attempt),
		})
	})
}

// RecordDelivered writes the delivery receipt and moves the job to delivered.
//
// The receipt and the state change are one transaction: a job can never be
// delivered without evidence, and evidence can never exist for a job that is
// not delivered.
func (l *Ledger) RecordDelivered(ctx context.Context, r Receipt, reconciled bool) error {
	return l.tx(ctx, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO delivery_receipts
				(job_id, destination_root, delivered_name, size_bytes,
				 content_fingerprint, attempt)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT (job_id) DO NOTHING`,
			r.JobID, r.DestinationRoot, r.DeliveredName, r.SizeBytes,
			r.Fingerprint, r.Attempt)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			UPDATE jobs
			   SET state = 'delivered',
			       reserved_name = $2,
			       terminal_at = COALESCE(terminal_at, now()),
			       failure_category = NULL,
			       updated_at = now()
			 WHERE job_id = $1`, r.JobID, r.DeliveredName); err != nil {
			return err
		}
		evt := jobs.EventDelivered
		if reconciled {
			evt = jobs.EventReconciled
		}
		return l.appendEvent(ctx, tx, eventInput{
			JobID:     r.JobID,
			EventType: evt,
			ToState:   ptr(string(jobs.StateDelivered)),
			Attempt:   ptr(r.Attempt),
		})
	})
}

// RecordUncertain marks a job whose publication could not be established.
//
// This is a terminal state without intervention, and deliberately so. The
// document may already be with the consumer; republishing it would risk a
// duplicate, and declaring failure would risk losing it. The contract requires
// the ambiguity to stay visible instead of being resolved by guessing.
func (l *Ledger) RecordUncertain(ctx context.Context, jobID string, category jobs.Category, attempt int) error {
	return l.tx(ctx, func(tx pgx.Tx) error {
		ct, err := tx.Exec(ctx, `
			UPDATE jobs
			   SET state = 'uncertain',
			       failure_category = $2,
			       terminal_at = COALESCE(terminal_at, now()),
			       updated_at = now()
			 WHERE job_id = $1`, jobID, string(category))
		if err != nil {
			return err
		}
		if ct.RowsAffected() == 0 {
			return ErrNotFound
		}
		return l.appendEvent(ctx, tx, eventInput{
			JobID:     jobID,
			EventType: jobs.EventUncertain,
			ToState:   ptr(string(jobs.StateUncertain)),
			Category:  ptr(string(category)),
			Attempt:   ptr(attempt),
		})
	})
}

// GetReceipt returns a job's delivery receipt, if one exists.
func (l *Ledger) GetReceipt(ctx context.Context, jobID string) (Receipt, error) {
	var r Receipt
	row := l.primary.QueryRow(ctx, `
		SELECT job_id, destination_root, delivered_name, size_bytes,
		       content_fingerprint, attempt, delivered_at, absent_observed_at
		  FROM delivery_receipts WHERE job_id = $1`, jobID)
	err := row.Scan(&r.JobID, &r.DestinationRoot, &r.DeliveredName, &r.SizeBytes,
		&r.Fingerprint, &r.Attempt, &r.DeliveredAt, &r.AbsentObservedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Receipt{}, ErrNotFound
	}
	if err != nil {
		return Receipt{}, fmt.Errorf("read delivery receipt: %w", err)
	}
	return r, nil
}

// NoteDeliveredFileAbsent records that a delivered file is no longer present.
//
// A delivered document disappearing from the consume directory is the normal
// consequence of Paperless ingesting it. Recording the observation keeps that
// visible without ever reinterpreting the delivery as a failure.
func (l *Ledger) NoteDeliveredFileAbsent(ctx context.Context, jobID string) error {
	_, err := l.primary.Exec(ctx, `
		UPDATE delivery_receipts
		   SET absent_observed_at = COALESCE(absent_observed_at, now())
		 WHERE job_id = $1`, jobID)
	if err != nil {
		return fmt.Errorf("note delivered file absent: %w", err)
	}
	return nil
}

// AlreadyRegistered reports whether a submission identity is already a job.
//
// Identity is root, name, inode and device together. A producer that reuses a
// filename for genuinely new content gets a new inode, and therefore a new
// job: distinct submissions stay distinct even when their names match.
func (l *Ledger) AlreadyRegistered(ctx context.Context, root, name string, inode, device uint64) (string, bool, error) {
	var jobID string
	row := l.primary.QueryRow(ctx, `
		SELECT job_id FROM jobs
		 WHERE source_root = $1 AND source_name = $2
		   AND source_inode = $3 AND source_device = $4`,
		root, name, int64(inode), int64(device))
	err := row.Scan(&jobID)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("look up submission identity: %w", err)
	}
	return jobID, true, nil
}

// JobsInState lists jobs in one state, oldest first, for recovery scans.
func (l *Ledger) JobsInState(ctx context.Context, state jobs.State, limit int) ([]Job, error) {
	rows, err := l.primary.Query(ctx, `
		SELECT `+jobColumns+`
		  FROM jobs WHERE state = $1 ORDER BY updated_at ASC LIMIT $2`,
		string(state), limit)
	if err != nil {
		return nil, fmt.Errorf("list jobs in state %s: %w", state, err)
	}
	defer rows.Close()

	var out []Job
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, job)
	}
	return out, rows.Err()
}

// CountReservations reports how many destination names are reserved.
//
// Reservations are never removed, so this number only grows. It is the
// retained consumed-name history: a name handed to one submission is never
// handed to another, even after Paperless removes the file.
func (l *Ledger) CountReservations(ctx context.Context) (int64, error) {
	var n int64
	if err := l.primary.QueryRow(ctx, `SELECT count(*) FROM name_reservations`).Scan(&n); err != nil {
		return 0, fmt.Errorf("count reservations: %w", err)
	}
	return n, nil
}

// LastDiscoveryRun reports when a submission was most recently registered.
//
// It is a proxy for discovery liveness that survives a process restart, which
// the in-process metric does not: a watcher that has been restarted reports a
// zero gauge but the ledger still knows when work last arrived.
func (l *Ledger) LastDiscoveryRun(ctx context.Context) (time.Time, error) {
	var ts *time.Time
	if err := l.primary.QueryRow(ctx, `SELECT max(discovered_at) FROM jobs`).Scan(&ts); err != nil {
		return time.Time{}, fmt.Errorf("read last discovery: %w", err)
	}
	if ts == nil {
		return time.Time{}, nil
	}
	return *ts, nil
}

// JobBySource returns the most recent job for a source root and name.
//
// It exists for tests and operator diagnostics. The running system never looks
// a job up by name: dispatch and delivery carry the opaque job id, precisely so
// that a filename never has to travel through the broker or a log line.
func (l *Ledger) JobBySource(ctx context.Context, root, name string) (Job, error) {
	row := l.primary.QueryRow(ctx, `
		SELECT `+jobColumns+`
		  FROM jobs WHERE source_root = $1 AND source_name = $2
		 ORDER BY created_at DESC LIMIT 1`, root, name)
	job, err := scanJob(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, ErrNotFound
	}
	if err != nil {
		return Job{}, fmt.Errorf("look up job by source: %w", err)
	}
	return job, nil
}

// DeliveredWithoutReceipt counts jobs marked delivered that have no receipt.
//
// It must always be zero: RecordDelivered writes the receipt and the state in
// one transaction. A non-zero answer means a delivered state appeared without
// the evidence that is supposed to accompany it.
func (l *Ledger) DeliveredWithoutReceipt(ctx context.Context) (int, error) {
	var n int
	err := l.primary.QueryRow(ctx, `
		SELECT count(*) FROM jobs j
		 WHERE j.state = 'delivered'
		   AND NOT EXISTS (SELECT 1 FROM delivery_receipts r WHERE r.job_id = j.job_id)`).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("count delivered jobs without a receipt: %w", err)
	}
	return n, nil
}

// UncertainWithoutPublishAttempt counts uncertain jobs for which no
// publication was ever attempted.
//
// It must always be zero: uncertainty is only reachable after the publish
// intent has been committed. A non-zero answer means uncertainty was inferred
// rather than observed.
func (l *Ledger) UncertainWithoutPublishAttempt(ctx context.Context) (int, error) {
	var n int
	err := l.primary.QueryRow(ctx, `
		SELECT count(*) FROM jobs j
		 WHERE j.state = 'uncertain'
		   AND j.publish_attempted_at IS NULL`).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("count uncertain jobs without a publication attempt: %w", err)
	}
	return n, nil
}

// DeliveredNames returns the set of names delivered into a destination root.
//
// It is the counterpart to reading the directory: a file in the destination
// that is not in this set was not put there by the pipeline.
func (l *Ledger) DeliveredNames(ctx context.Context, root string) (map[string]bool, error) {
	rows, err := l.primary.Query(ctx,
		`SELECT delivered_name FROM delivery_receipts WHERE destination_root = $1`, root)
	if err != nil {
		return nil, fmt.Errorf("read delivered names: %w", err)
	}
	defer rows.Close()

	out := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		out[name] = true
	}
	return out, rows.Err()
}
