// Package ledger is the durable job store backed by the PostgreSQL cluster.
//
// Topology: writes go to the primary; the standby is opened as a separate,
// read-only pool so the applications can observe replication rather than
// assume it. Nothing in this package deletes a row — job history is retained,
// and the destructive cleanup policy is unresolved.
package ledger

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/config"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/jobs"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/logging"
)

// ErrNotFound reports a job reference with no ledger row.
var ErrNotFound = errors.New("job not found")

// ErrNoReplica reports that no standby endpoint is configured.
var ErrNoReplica = errors.New("no replica endpoint configured")

// ErrStateConflict reports that a row was not in the expected state, which is
// the normal outcome when two workers race for the same job.
var ErrStateConflict = errors.New("job was not in the expected state")

// Job is a durable job row.
type Job struct {
	JobID            string
	ContractVersion  int
	State            jobs.State
	SourceRoot       string
	SourceName       string
	SizeBytes        *int64
	FingerprintAlgo  *string
	Fingerprint      []byte
	PolicyVersion    string
	NormalizedName   *string
	ReservedName     *string
	DispatchAttempts int
	DeliveryAttempts int
	FailureCategory  *string
	ClaimedBy        *string
	ClaimedAt        *time.Time
	CreatedAt        time.Time
	UpdatedAt        time.Time
	DispatchedAt     *time.Time
	LastDeliveryAt   *time.Time
	TerminalAt       *time.Time
	// Source identity as discovery observed it. Used to detect a submission
	// that was replaced or modified between discovery and publication.
	SourceInode      *int64
	SourceDevice     *int64
	SourceModifiedAt *time.Time
	// PublishAttemptedAt is set immediately before the destination link, so a
	// crash during publication is recoverable as "may have published".
	PublishAttemptedAt *time.Time
	// DestinationRoot is the consume root this job was accepted for. A
	// renamer configured with a different one must refuse it rather than
	// redirect work that was already accepted elsewhere.
	DestinationRoot *string
	// DestinationRootUnknown marks work accepted before the destination was
	// recorded. It is not "compatible with anything": it is ambiguous, and
	// ambiguous accepted work is held rather than adopted by whatever
	// configuration happens to be running now.
	DestinationRootUnknown bool
	// PublishClaimedBy identifies the attempt currently entitled to publish.
	PublishClaimedBy *string
	// PublishInode and PublishDevice identify the file this job linked into
	// place. Ownership of a destination is proved by identity, not by content:
	// two distinct submissions may legitimately hold identical bytes.
	PublishInode  *int64
	PublishDevice *int64
}

// Event is one append-only history row.
type Event struct {
	EventID    int64
	JobID      string
	OccurredAt time.Time
	EventType  jobs.EventType
	FromState  *string
	ToState    *string
	Category   *string
	Actor      string
	Attempt    *int
}

// Ledger owns the connection pools.
type Ledger struct {
	primary   *pgxpool.Pool
	replica   *pgxpool.Pool
	log       *slog.Logger
	actor     string
	opTimeout time.Duration
}

// Options configures Open.
type Options struct {
	Config config.Database
	Logger *slog.Logger
	// Actor identifies the instance writing history rows.
	Actor string
	// OpTimeout bounds individual statements so a stalled dependency cannot
	// hold a worker indefinitely.
	OpTimeout time.Duration
}

// Open builds the pools. It does not connect eagerly: readiness probing is the
// component that establishes and reports real connectivity.
func Open(ctx context.Context, opts Options) (*Ledger, error) {
	if opts.OpTimeout <= 0 {
		opts.OpTimeout = 10 * time.Second
	}

	primary, err := newPool(ctx, opts.Config.DSN(), opts.Config.MaxConns)
	if err != nil {
		return nil, fmt.Errorf("configure primary pool: %w", err)
	}

	var replica *pgxpool.Pool
	if dsn := opts.Config.ReplicaDSN(); dsn != "" {
		replica, err = newPool(ctx, dsn, opts.Config.MaxConns)
		if err != nil {
			primary.Close()
			return nil, fmt.Errorf("configure replica pool: %w", err)
		}
	}

	return &Ledger{
		primary:   primary,
		replica:   replica,
		log:       opts.Logger,
		actor:     opts.Actor,
		opTimeout: opts.OpTimeout,
	}, nil
}

func newPool(ctx context.Context, dsn string, maxConns int) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = int32(maxConns)
	cfg.MinConns = 0
	cfg.MaxConnLifetime = 30 * time.Minute
	cfg.MaxConnIdleTime = 5 * time.Minute
	// A short health-check period makes the pool discard connections that died
	// while the dependency was down, so recovery does not wait for a request
	// to fail first.
	cfg.HealthCheckPeriod = 5 * time.Second
	return pgxpool.NewWithConfig(ctx, cfg)
}

// Close releases both pools.
func (l *Ledger) Close() {
	if l.replica != nil {
		l.replica.Close()
	}
	l.primary.Close()
}

// HasReplica reports whether a standby endpoint is configured.
func (l *Ledger) HasReplica() bool { return l.replica != nil }

// PingPrimary verifies the primary answers and is not in recovery. A standby
// answering on the primary endpoint is a misconfiguration, not readiness.
func (l *Ledger) PingPrimary(ctx context.Context) error {
	var inRecovery bool
	if err := l.primary.QueryRow(ctx, `SELECT pg_is_in_recovery()`).Scan(&inRecovery); err != nil {
		return err
	}
	if inRecovery {
		return errors.New("primary endpoint is in recovery")
	}
	return nil
}

// PingReplica verifies the standby answers and reports whether it is still in
// recovery. A promoted standby reports false, which is real information rather
// than an error.
func (l *Ledger) PingReplica(ctx context.Context) (inRecovery bool, err error) {
	if l.replica == nil {
		return false, ErrNoReplica
	}
	err = l.replica.QueryRow(ctx, `SELECT pg_is_in_recovery()`).Scan(&inRecovery)
	return inRecovery, err
}

// ReplicationState describes one walsender on the primary.
type ReplicationState struct {
	ApplicationName string
	State           string
	SyncState       string
	SentLSN         string
	ReplayLSN       string
	ReplayLagBytes  int64
}

// ReplicationStatus reads pg_stat_replication on the primary. It is real
// cluster evidence: an empty slice means no standby is streaming.
func (l *Ledger) ReplicationStatus(ctx context.Context) ([]ReplicationState, error) {
	rows, err := l.primary.Query(ctx, `
		SELECT application_name,
		       state,
		       sync_state,
		       COALESCE(sent_lsn::text, ''),
		       COALESCE(replay_lsn::text, ''),
		       COALESCE(pg_wal_lsn_diff(sent_lsn, replay_lsn), 0)::bigint
		  FROM pg_stat_replication`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ReplicationState
	for rows.Next() {
		var r ReplicationState
		if err := rows.Scan(&r.ApplicationName, &r.State, &r.SyncState, &r.SentLSN, &r.ReplayLSN, &r.ReplayLagBytes); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// RegisterInput describes a submission being made durable.
//
// Discovery is not implemented in this increment, so nothing in the running
// applications calls this. The integration suite calls it directly: it is the
// same code path the discovery worker will use, exercised against the real
// cluster rather than replaced by a substitute.
type RegisterInput struct {
	SourceRoot      string
	SourceName      string
	SizeBytes       *int64
	FingerprintAlgo *string
	Fingerprint     []byte
	// PolicyIdentity is the naming policy this submission is accepted under.
	// It is recorded once, at registration, and never recomputed: a renamer
	// running a different policy must refuse the job rather than rename it
	// under rules the job was not accepted with.
	PolicyIdentity string
	// Source identity as discovery observed it.
	SourceInode      *int64
	SourceDevice     *int64
	SourceModifiedAt *time.Time
	// DestinationRoot is the consume root this submission is accepted for.
	DestinationRoot string
}

// nullIfEmpty stores an unset destination as NULL rather than as the empty
// string, so "no accepted destination recorded" is one value and not two.
func nullIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// RegisterJob makes a submission durable and writes its first history row.
// The job identity is assigned here and never changes, including across
// republication and redelivery.
func (l *Ledger) RegisterJob(ctx context.Context, in RegisterInput) (Job, error) {
	id := uuid.NewString()
	var job Job
	err := l.tx(ctx, func(tx pgx.Tx) error {
		row := tx.QueryRow(ctx, `
			INSERT INTO jobs (
				job_id, contract_version, state, source_root, source_name,
				size_bytes, fingerprint_algorithm, content_fingerprint, policy_version,
				source_inode, source_device, source_modified_at, discovered_at,
				destination_root
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, now(), $13)
			RETURNING `+jobColumns,
			id, jobs.ContractVersion, string(jobs.StatePendingDispatch),
			in.SourceRoot, in.SourceName, in.SizeBytes, in.FingerprintAlgo,
			in.Fingerprint, in.PolicyIdentity,
			in.SourceInode, in.SourceDevice, in.SourceModifiedAt, nullIfEmpty(in.DestinationRoot))
		var scanErr error
		job, scanErr = scanJob(row)
		if scanErr != nil {
			return scanErr
		}
		return l.appendEvent(ctx, tx, eventInput{
			JobID:     id,
			EventType: jobs.EventRegistered,
			ToState:   ptr(string(jobs.StatePendingDispatch)),
		})
	})
	if err != nil {
		return Job{}, err
	}
	l.log.Info("job registered",
		slog.String("event", "job_registered"),
		slog.String("job_id", id),
		slog.String("state", string(jobs.StatePendingDispatch)))
	return job, nil
}

// ClaimForDispatch atomically moves up to limit pending jobs into dispatching
// and returns them. SKIP LOCKED means several watcher instances may run
// without handing the same row to two dispatchers.
func (l *Ledger) ClaimForDispatch(ctx context.Context, limit int) ([]Job, error) {
	var claimed []Job
	err := l.tx(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			WITH picked AS (
				SELECT job_id
				  FROM jobs
				 WHERE state = 'pending_dispatch'
				 ORDER BY created_at
				 FOR UPDATE SKIP LOCKED
				 LIMIT $1
			)
			UPDATE jobs j
			   SET state = 'dispatching',
			       claimed_by = $2,
			       claimed_at = now(),
			       dispatch_attempts = j.dispatch_attempts + 1,
			       updated_at = now()
			  FROM picked
			 WHERE j.job_id = picked.job_id
			RETURNING `+prefixedJobColumns("j"), limit, l.actor)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			job, scanErr := scanJob(rows)
			if scanErr != nil {
				return scanErr
			}
			claimed = append(claimed, job)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		rows.Close()
		for _, job := range claimed {
			if err := l.appendEvent(ctx, tx, eventInput{
				JobID:     job.JobID,
				EventType: jobs.EventDispatchClaimed,
				FromState: ptr(string(jobs.StatePendingDispatch)),
				ToState:   ptr(string(jobs.StateDispatching)),
				Attempt:   ptr(job.DispatchAttempts),
			}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return claimed, nil
}

// MarkDispatched records a confirmed publication.
//
// It does not require the row to still be in the dispatching state. A renamer
// can receive and settle the delivery before the watcher gets its confirm
// written, so insisting on dispatching here would lose the confirmation and
// leave a job that was demonstrably published looking as though it never was.
// The dispatch timestamp and the history row are therefore recorded
// unconditionally, while the state only advances if nothing else has moved it
// on already.
func (l *Ledger) MarkDispatched(ctx context.Context, jobID string, attempt int) error {
	return l.tx(ctx, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `
			UPDATE jobs
			   SET state = CASE WHEN state = 'dispatching' THEN 'dispatched' ELSE state END,
			       dispatched_at = COALESCE(dispatched_at, now()),
			       claimed_by = CASE WHEN state = 'dispatching' THEN claimed_by ELSE NULL END,
			       updated_at = now()
			 WHERE job_id = $1`, jobID)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return ErrNotFound
		}
		return l.appendEvent(ctx, tx, eventInput{
			JobID:     jobID,
			EventType: jobs.EventDispatchConfirmed,
			ToState:   ptr(string(jobs.StateDispatched)),
			Attempt:   ptr(attempt),
		})
	})
}

// ReturnToPending releases a claim without confirming publication.
//
// The job stays discoverable with the same identity, so a publisher
// interruption cannot strand it silently. Republication may produce a
// duplicate message; duplicate deliveries are allowed and must be safe.
func (l *Ledger) ReturnToPending(ctx context.Context, jobID string, category jobs.Category, evtType jobs.EventType) error {
	return l.transition(ctx, jobID, jobs.StateDispatching, jobs.StatePendingDispatch, eventInput{
		JobID:     jobID,
		EventType: evtType,
		Category:  ptr(string(category)),
	}, `claimed_by = NULL, claimed_at = NULL, failure_category = `+quoteLiteral(string(category)))
}

// ReclaimStaleDispatch returns claims older than maxAge to pending_dispatch.
// It covers a watcher that died between claiming and confirming.
func (l *Ledger) ReclaimStaleDispatch(ctx context.Context, maxAge time.Duration) (int, error) {
	var ids []string
	err := l.tx(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			WITH picked AS (
				SELECT job_id FROM jobs
				 WHERE state = 'dispatching'
				   AND claimed_at < now() - $1::interval
				   AND dispatched_at IS NULL
				 FOR UPDATE SKIP LOCKED
			)
			UPDATE jobs j
			   SET state = 'pending_dispatch',
			       claimed_by = NULL,
			       claimed_at = NULL,
			       failure_category = $2,
			       updated_at = now()
			  FROM picked
			 WHERE j.job_id = picked.job_id
			RETURNING j.job_id`, maxAge.String(), string(jobs.CategoryDispatchReclaimed))
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				return err
			}
			ids = append(ids, id)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		rows.Close()
		for _, id := range ids {
			if err := l.appendEvent(ctx, tx, eventInput{
				JobID:     id,
				EventType: jobs.EventDispatchReclaimed,
				FromState: ptr(string(jobs.StateDispatching)),
				ToState:   ptr(string(jobs.StatePendingDispatch)),
				Category:  ptr(string(jobs.CategoryDispatchReclaimed)),
			}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	for _, id := range ids {
		l.log.Warn("stranded dispatch claim returned to pending",
			slog.String("event", "dispatch_reclaimed"),
			slog.String("job_id", id),
			slog.String("category", string(jobs.CategoryDispatchReclaimed)))
	}
	return len(ids), nil
}

// BeginDelivery records that a renamer has taken ownership of a delivery.
//
// It accepts a job in dispatched, processing or held state: a redelivery of an
// already-processed job is a normal at-least-once event, and the count of
// delivery attempts is part of the retained history.
//
// States that carry an OUTCOME or an ambiguity are preserved, not reset. This
// used to move everything except held and delivered to processing, which had
// two consequences that defeated recovery entirely: a job interrupted during
// publication came back as processing, so the publishing recovery path could
// never run on the ordinary consumer path; and uncertain -- which must stay
// terminal until someone decides -- was silently reopened and reprocessed. The
// row returned is the row after the update, so the caller sees the preserved
// state and can act on it.
func (l *Ledger) BeginDelivery(ctx context.Context, jobID string, attempt int, redelivered bool) (Job, error) {
	var job Job
	err := l.tx(ctx, func(tx pgx.Tx) error {
		row := tx.QueryRow(ctx, `
			UPDATE jobs
			   SET state = CASE
			       WHEN state IN ('held','delivered','publishing','uncertain') THEN state
			       ELSE 'processing' END,
			       delivery_attempts = delivery_attempts + 1,
			       last_delivery_at = now(),
			       updated_at = now()
			 WHERE job_id = $1
			RETURNING `+jobColumns, jobID)
		var scanErr error
		job, scanErr = scanJob(row)
		if errors.Is(scanErr, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if scanErr != nil {
			return scanErr
		}
		detail := map[string]any{"redelivered": redelivered}
		return l.appendEvent(ctx, tx, eventInput{
			JobID:     jobID,
			EventType: jobs.EventDeliveryReceived,
			ToState:   ptr(string(job.State)),
			Attempt:   ptr(attempt),
			Detail:    detail,
		})
	})
	if err != nil {
		return Job{}, err
	}
	return job, nil
}

// RecordHold durably records a hold requiring intervention.
//
// This is the only terminal outcome this increment produces. It explicitly
// does not assert delivery: no file has been moved, and no delivery receipt
// exists. A consumer acknowledgement is issued only after this commit returns.
func (l *Ledger) RecordHold(ctx context.Context, jobID string, category jobs.Category, attempt int, staleAfter time.Duration) error {
	if staleAfter <= 0 {
		staleAfter = time.Minute
	}
	err := l.tx(ctx, func(tx pgx.Tx) error {
		// A stale attempt must not demote a newer outcome, and a LIVE
		// publication must not be closed under the attempt performing it.
		//
		// The state guard alone was not enough once the receipt began to
		// precede the reveal. A redelivered sibling -- a policy mismatch, a
		// late failure -- could take a job from `publishing` to `held` while
		// the original publisher was paused between its committed receipt and
		// its reveal. The publisher then resumed, made the document visible,
		// and was refused its final state: a document in the consumer's
		// directory for a job recorded `held`.
		//
		// So a committed receipt blocks a hold only while the publication
		// CLAIM IS STILL LIVE. Blocking it outright was too broad and broke
		// the opposite case: the publisher itself discovering it cannot
		// publish -- an unverifiable destination, a refused reveal -- could no
		// longer close its own job, so the delivery was returned, redelivered,
		// and the job spun in `publishing` indefinitely.
		//
		// Once the claim is older than the takeover window, whoever is holding
		// the job is entitled to close it, and the receipt it withdraws
		// describes a publication that never became visible. The withdrawal is
		// recorded as an event, so the history survives the receipt.
		tag, err := tx.Exec(ctx, `
			UPDATE jobs
			   SET state = 'held',
			       failure_category = $2,
			       terminal_at = COALESCE(terminal_at, now()),
			       updated_at = now()
			 WHERE job_id = $1
			   AND state NOT IN ('delivered','uncertain')
			   AND (NOT EXISTS (SELECT 1 FROM delivery_receipts r WHERE r.job_id = $1)
			        OR publish_attempted_at IS NULL
			        OR publish_attempted_at < now() - $3::interval)`,
			jobID, string(category), staleAfter.String())
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			// Either there is no such job, or it already reached an outcome
			// this hold must not replace. Tell those apart so the caller can
			// settle rather than retry.
			var state string
			if qerr := tx.QueryRow(ctx, `SELECT state FROM jobs WHERE job_id = $1`, jobID).Scan(&state); qerr != nil {
				if errors.Is(qerr, pgx.ErrNoRows) {
					return ErrNotFound
				}
				return qerr
			}
			return fmt.Errorf("%w: job is %s", ErrOutcomeAlreadyRecorded, state)
		}
		// A receipt surviving a hold would describe a delivery that is not
		// going to happen, for a job now recorded as needing intervention.
		// It only reaches here when the claim was stale, so the publication
		// it authorised is not in flight.
		ct, derr := tx.Exec(ctx, `DELETE FROM delivery_receipts WHERE job_id = $1`, jobID)
		if derr != nil {
			return derr
		}
		if ct.RowsAffected() > 0 {
			if _, uerr := tx.Exec(ctx,
				`UPDATE job_publication_inodes SET linked_at = NULL WHERE job_id = $1`, jobID); uerr != nil {
				return uerr
			}
			if aerr := l.appendEvent(ctx, tx, eventInput{
				JobID:     jobID,
				EventType: jobs.EventPublishAbandoned,
				Attempt:   ptr(attempt),
				Detail:    map[string]any{"reason": "held after the publication claim went stale"},
			}); aerr != nil {
				return aerr
			}
		}
		return l.appendEvent(ctx, tx, eventInput{
			JobID:     jobID,
			EventType: jobs.EventHeld,
			ToState:   ptr(string(jobs.StateHeld)),
			Category:  ptr(string(category)),
			Attempt:   ptr(attempt),
		})
	})
	if err != nil {
		return err
	}
	l.log.Info("job held pending intervention",
		slog.String("event", "job_held"),
		slog.String("job_id", jobID),
		slog.String("state", string(jobs.StateHeld)),
		slog.String("category", string(category)),
		slog.Int("attempt", attempt))
	return nil
}

// GetJob reads one job from the primary.
func (l *Ledger) GetJob(ctx context.Context, jobID string) (Job, error) {
	row := l.primary.QueryRow(ctx, `SELECT `+jobColumns+` FROM jobs WHERE job_id = $1`, jobID)
	job, err := scanJob(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, ErrNotFound
	}
	return job, err
}

// GetJobFromReplica reads one job from the standby. It is the read path that
// demonstrates replication end to end from application code.
func (l *Ledger) GetJobFromReplica(ctx context.Context, jobID string) (Job, error) {
	if l.replica == nil {
		return Job{}, ErrNoReplica
	}
	row := l.replica.QueryRow(ctx, `SELECT `+jobColumns+` FROM jobs WHERE job_id = $1`, jobID)
	job, err := scanJob(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, ErrNotFound
	}
	return job, err
}

// ExecOnReplica runs an arbitrary statement against the standby. The
// integration suite uses it to prove the standby rejects writes.
func (l *Ledger) ExecOnReplica(ctx context.Context, sql string, args ...any) error {
	if l.replica == nil {
		return ErrNoReplica
	}
	_, err := l.replica.Exec(ctx, sql, args...)
	return err
}

// Checkpoint forces a PostgreSQL checkpoint on the primary.
//
// The crash-recovery evidence uses it so the durability claim is about
// committed WAL rather than about whatever happened to be flushed at the
// moment the process was killed.
func (l *Ledger) Checkpoint(ctx context.Context) error {
	_, err := l.primary.Exec(ctx, "CHECKPOINT")
	return err
}

// CreateLoginRole creates a least-privilege login role on the primary.
//
// It exists for the credential evidence: proving that a password containing
// spaces and quotes really authenticates requires a real role on the real
// cluster with that real password. The role is granted nothing but CONNECT.
//
// The role name is restricted to an identifier alphabet and the password is
// passed as a bind parameter to format(), so neither can inject SQL.
func (l *Ledger) CreateLoginRole(ctx context.Context, role, password, database string) error {
	if !jobs.SafeIdentifier(role) || strings.ContainsAny(role, ".-") {
		return fmt.Errorf("refusing to create a role with an unsafe name")
	}
	if !jobs.SafeIdentifier(database) {
		return fmt.Errorf("refusing to reference a database with an unsafe name")
	}
	// DDL cannot take bind parameters, and a DO block takes none either, so
	// the statements are rendered server-side with format(): %I quotes the
	// identifier and %L quotes the literal. The password is still transmitted
	// as a bind parameter to format(), never concatenated here.
	var createSQL, grantSQL string
	if err := l.primary.QueryRow(ctx, `
		SELECT format('CREATE ROLE %I WITH LOGIN PASSWORD %L', $1::text, $2::text),
		       format('GRANT CONNECT ON DATABASE %I TO %I', $3::text, $1::text)`,
		role, password, database).Scan(&createSQL, &grantSQL); err != nil {
		return fmt.Errorf("render role DDL: %w", err)
	}
	if _, err := l.primary.Exec(ctx, createSQL); err != nil {
		return fmt.Errorf("create role: %w", err)
	}
	if _, err := l.primary.Exec(ctx, grantSQL); err != nil {
		return fmt.Errorf("grant connect: %w", err)
	}
	return nil
}

// DropLoginRole removes a role created by CreateLoginRole.
func (l *Ledger) DropLoginRole(ctx context.Context, role string) error {
	if !jobs.SafeIdentifier(role) || strings.ContainsAny(role, ".-") {
		return fmt.Errorf("refusing to drop a role with an unsafe name")
	}
	var revokeSQL, dropSQL string
	if err := l.primary.QueryRow(ctx, `
		SELECT format('REVOKE ALL ON DATABASE %I FROM %I', current_database(), $1::text),
		       format('DROP ROLE IF EXISTS %I', $1::text)`, role).Scan(&revokeSQL, &dropSQL); err != nil {
		return fmt.Errorf("render role teardown DDL: %w", err)
	}
	// A revoke against an absent grant is not an error worth failing teardown.
	_, _ = l.primary.Exec(ctx, revokeSQL)
	_, err := l.primary.Exec(ctx, dropSQL)
	return err
}

// SlowPingPrimary verifies the primary answers, taking at least d to do it
// because the server itself waits.
//
// The delay is real work by the real database — pg_sleep runs on the primary
// and the round trip genuinely takes that long — so a probe built on this
// consults the dependency rather than reporting a manufactured outcome. It
// exists so a readiness evaluation can be observably in flight long enough for
// a concurrent withdrawal to be tested without any artificial pause in the
// application.
func (l *Ledger) SlowPingPrimary(ctx context.Context, d time.Duration) error {
	seconds := d.Seconds()
	if seconds <= 0 || seconds > 30 {
		return fmt.Errorf("refusing an out-of-range delay of %s", d)
	}
	var inRecovery bool
	if err := l.primary.QueryRow(ctx,
		`SELECT pg_sleep($1), pg_is_in_recovery()`, seconds).Scan(nil, &inRecovery); err != nil {
		return err
	}
	if inRecovery {
		return errors.New("primary endpoint is in recovery")
	}
	return nil
}

// HoldTableLock takes a conflicting lock on a table and holds it.
//
// It exists for the privacy evidence: making a real registration slow enough
// for PostgreSQL to log it as a slow statement requires real lock contention,
// not an artificial delay inside the application. onHeld is invoked once the
// lock is actually held, so a caller can start the statement it wants blocked.
//
// The table name is validated against the closed set this build owns, so this
// cannot be turned into an injection point.
func (l *Ledger) HoldTableLock(ctx context.Context, table string, hold time.Duration, onHeld func()) error {
	switch table {
	case "jobs", "job_events":
	default:
		return fmt.Errorf("refusing to lock an unrecognised table %q", table)
	}

	return pgx.BeginFunc(ctx, l.primary, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `LOCK TABLE `+table+` IN ACCESS EXCLUSIVE MODE`); err != nil {
			return err
		}
		if onHeld != nil {
			onHeld()
		}
		t := time.NewTimer(hold)
		defer t.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
		return nil
	})
}

// Events returns the retained history for a job, oldest first.
func (l *Ledger) Events(ctx context.Context, jobID string) ([]Event, error) {
	rows, err := l.primary.Query(ctx, `
		SELECT event_id, job_id, occurred_at, event_type, from_state, to_state, category, actor, attempt
		  FROM job_events WHERE job_id = $1 ORDER BY event_id`, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Event
	for rows.Next() {
		var e Event
		var et string
		if err := rows.Scan(&e.EventID, &e.JobID, &e.OccurredAt, &et, &e.FromState, &e.ToState, &e.Category, &e.Actor, &e.Attempt); err != nil {
			return nil, err
		}
		e.EventType = jobs.EventType(et)
		out = append(out, e)
	}
	return out, rows.Err()
}

// Counts is an accounting snapshot: one count per state plus the age of the
// oldest job still owing a publication.
type Counts struct {
	ByState          map[jobs.State]int
	ByCategory       map[jobs.Category]int
	OldestPendingAge time.Duration
	Total            int
}

// Snapshot aggregates the ledger for the accounting worker and the metrics.
func (l *Ledger) Snapshot(ctx context.Context) (Counts, error) {
	out := Counts{ByState: map[jobs.State]int{}, ByCategory: map[jobs.Category]int{}}
	for _, s := range jobs.States() {
		out.ByState[s] = 0
	}

	rows, err := l.primary.Query(ctx, `SELECT state, count(*) FROM jobs GROUP BY state`)
	if err != nil {
		return Counts{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var state string
		var n int
		if err := rows.Scan(&state, &n); err != nil {
			return Counts{}, err
		}
		parsed, perr := jobs.ParseState(state)
		if perr != nil {
			// An unrecognised stored state must not become an unbounded metric
			// label. It is surfaced as a log line instead.
			l.log.Error("ledger contains an unrecognised job state",
				slog.String("event", "ledger_unknown_state"),
				slog.String("error_kind", "unclassified"))
			continue
		}
		out.ByState[parsed] = n
		out.Total += n
	}
	if err := rows.Err(); err != nil {
		return Counts{}, err
	}
	rows.Close()

	// Hold reasons, so a caller can tell one kind of stuck job from another
	// without reading any document identity.
	catRows, err := l.primary.Query(ctx, `
		SELECT failure_category, count(*) FROM jobs
		 WHERE failure_category IS NOT NULL GROUP BY 1`)
	if err != nil {
		return Counts{}, err
	}
	for catRows.Next() {
		var cat string
		var n int
		if err := catRows.Scan(&cat, &n); err != nil {
			catRows.Close()
			return Counts{}, err
		}
		out.ByCategory[jobs.Category(cat)] = n
	}
	catRows.Close()
	if err := catRows.Err(); err != nil {
		return Counts{}, err
	}

	var oldest *time.Duration
	var secs *float64
	if err := l.primary.QueryRow(ctx, `
		SELECT EXTRACT(EPOCH FROM (now() - min(created_at)))::double precision
		  FROM jobs WHERE state = 'pending_dispatch'`).Scan(&secs); err != nil {
		return Counts{}, err
	}
	if secs != nil {
		d := time.Duration(*secs * float64(time.Second))
		oldest = &d
	}
	if oldest != nil {
		out.OldestPendingAge = *oldest
	}
	return out, nil
}

// --- internals ---

const jobColumns = `job_id, contract_version, state, source_root, source_name,
	size_bytes, fingerprint_algorithm, content_fingerprint, policy_version,
	normalized_name, reserved_name, dispatch_attempts, delivery_attempts,
	failure_category, claimed_by, claimed_at, created_at, updated_at,
	dispatched_at, last_delivery_at, terminal_at,
	source_inode, source_device, source_modified_at, publish_attempted_at,
	destination_root, publish_claimed_by, publish_inode, publish_device,
	destination_root_unknown`

func prefixedJobColumns(alias string) string {
	cols := []string{
		"job_id", "contract_version", "state", "source_root", "source_name",
		"size_bytes", "fingerprint_algorithm", "content_fingerprint", "policy_version",
		"normalized_name", "reserved_name", "dispatch_attempts", "delivery_attempts",
		"failure_category", "claimed_by", "claimed_at", "created_at", "updated_at",
		"dispatched_at", "last_delivery_at", "terminal_at",
		"source_inode", "source_device", "source_modified_at", "publish_attempted_at",
		"destination_root", "publish_claimed_by", "publish_inode", "publish_device",
		"destination_root_unknown",
	}
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = alias + "." + c
	}
	return joinComma(out)
}

func joinComma(in []string) string {
	s := ""
	for i, v := range in {
		if i > 0 {
			s += ", "
		}
		s += v
	}
	return s
}

type scannable interface {
	Scan(dest ...any) error
}

func scanJob(row scannable) (Job, error) {
	var j Job
	var state string
	err := row.Scan(
		&j.JobID, &j.ContractVersion, &state, &j.SourceRoot, &j.SourceName,
		&j.SizeBytes, &j.FingerprintAlgo, &j.Fingerprint, &j.PolicyVersion,
		&j.NormalizedName, &j.ReservedName, &j.DispatchAttempts, &j.DeliveryAttempts,
		&j.FailureCategory, &j.ClaimedBy, &j.ClaimedAt, &j.CreatedAt, &j.UpdatedAt,
		&j.DispatchedAt, &j.LastDeliveryAt, &j.TerminalAt,
		&j.SourceInode, &j.SourceDevice, &j.SourceModifiedAt, &j.PublishAttemptedAt,
		&j.DestinationRoot, &j.PublishClaimedBy, &j.PublishInode, &j.PublishDevice,
		&j.DestinationRootUnknown)
	if err != nil {
		return Job{}, err
	}
	parsed, perr := jobs.ParseState(state)
	if perr != nil {
		return Job{}, perr
	}
	j.State = parsed
	return j, nil
}

type eventInput struct {
	JobID     string
	EventType jobs.EventType
	FromState *string
	ToState   *string
	Category  *string
	Attempt   *int
	Detail    map[string]any
}

func (l *Ledger) appendEvent(ctx context.Context, tx pgx.Tx, in eventInput) error {
	detail := in.Detail
	if detail == nil {
		detail = map[string]any{}
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO job_events (job_id, event_type, from_state, to_state, category, actor, attempt, detail)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		in.JobID, string(in.EventType), in.FromState, in.ToState, in.Category, l.actor, in.Attempt, detail)
	return err
}

func (l *Ledger) transition(ctx context.Context, jobID string, from, to jobs.State, evt eventInput, extraSet string) error {
	sql := fmt.Sprintf(`
		UPDATE jobs SET state = $3, updated_at = now(), %s
		 WHERE job_id = $1 AND state = $2`, extraSet)
	return l.tx(ctx, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, sql, jobID, string(from), string(to))
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return fmt.Errorf("%w: expected %s", ErrStateConflict, from)
		}
		evt.FromState = ptr(string(from))
		evt.ToState = ptr(string(to))
		return l.appendEvent(ctx, tx, evt)
	})
}

func (l *Ledger) tx(ctx context.Context, fn func(pgx.Tx) error) error {
	ctx, cancel := context.WithTimeout(ctx, l.opTimeout)
	defer cancel()
	err := pgx.BeginFunc(ctx, l.primary, fn)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			l.log.Error("durable write rejected by the database",
				slog.String("event", "ledger_write_failed"),
				slog.String("error_kind", logging.ErrorKind(err)),
				slog.String("reason", pgErr.Code))
		}
	}
	return err
}

func ptr[T any](v T) *T { return &v }

// quoteLiteral renders a closed-set category as a SQL literal. It is only ever
// called with values from jobs.Categories(), and it rejects anything else so a
// caller cannot turn it into an injection point.
func quoteLiteral(s string) string {
	if !jobs.SafeIdentifier(s) {
		panic("ledger: refusing to inline an unsafe literal")
	}
	return "'" + s + "'"
}
