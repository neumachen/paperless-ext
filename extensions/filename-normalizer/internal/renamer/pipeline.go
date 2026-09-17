package renamer

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/config"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/jobs"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/ledger"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/naming"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/storage"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/telemetry"
)

// Pipeline takes one job from a delivery to a durable outcome.
//
// # The guarantee it keeps
//
// Every path below ends in a committed ledger row before the delivery is
// settled, and no path ever overwrites a file. The publication step is a
// link(2) into the consume directory, which fails if the name is taken, so the
// "is it free?" question and the "take it" action are one atomic step decided
// by the kernel rather than a check followed by a racy write.
//
// # The ordering that makes recovery possible
//
//	reserve name  ->  record publish intent  ->  link  ->  record receipt
//
// The intent is committed before the link and the receipt after it. A process
// that dies anywhere in between leaves a job in `publishing`, which is the
// honest statement "a publication may have happened". Recovery then looks at
// the destination itself:
//
//   - present with this job's content  -> reconcile to delivered, no republish
//   - present with other content       -> destination_conflict, nothing touched
//   - absent                           -> uncertain, and NOT redelivered,
//     because the consumer may already have taken it
//
// # Concurrent attempts
//
// Two workers can hold the same job at once: a redelivery reaches a second
// consumer while the first is still running, and a worker that lost its broker
// connection keeps its filesystem access. Nothing here may therefore act on
// another attempt's in-progress bytes. Two rules enforce that:
//
//   - every attempt writes into a file only it knows the name of, and a
//     working copy becomes visible under the job's canonical name only by
//     link(2), after it has been verified. An attempt never unlinks, truncates
//     or overwrites a file another attempt may be writing.
//   - the ledger refuses to demote a newer outcome, so a slow attempt that
//     fails after another has delivered cannot replace `delivered` with
//     `held`.
type Pipeline struct {
	cfg     config.RenamerConfig
	led     *ledger.Ledger
	log     *slog.Logger
	metrics *telemetry.Metrics
	// faults injects real process-level interruptions at named points. It is
	// empty unless a deployment sets FN_FAULT_POINTS, and it exists so the
	// interruption window between the link and the receipt can be exercised
	// for real rather than described.
	faults config.FaultPoints
}

// NewPipeline builds the processing pipeline.
func NewPipeline(cfg config.RenamerConfig, led *ledger.Ledger, log *slog.Logger, m *telemetry.Metrics) *Pipeline {
	return &Pipeline{cfg: cfg, led: led, log: log, metrics: m, faults: cfg.Faults}
}

// Outcome is what the pipeline decided.
type Outcome struct {
	// Settled is true when a durable outcome was recorded and the delivery may
	// be acknowledged. False means nothing was committed and the delivery must
	// go back to the broker.
	Settled bool
	// Label is the delivery-outcome metric label.
	Label string
	// State is the durable state reached, when Settled.
	State jobs.State
	// Category is the closed-set reason, when there is one.
	Category jobs.Category
	// Err explains an unsettled outcome. It never carries document text.
	Err error
}

func settled(label string, state jobs.State, cat jobs.Category) Outcome {
	return Outcome{Settled: true, Label: label, State: state, Category: cat}
}

func unsettled(err error) Outcome {
	return Outcome{Settled: false, Label: "requeued", Err: err}
}

// tempPrefix marks a short-lived, attempt-private file.
//
// It is a dotfile because the consume directory is watched by a consumer, and
// the conventional signal for "not a submission" there is a leading dot. Every
// such file also carries a per-attempt nonce, so two attempts on one job never
// share a temporary name.
const tempPrefix = ".fn-"

// nonce returns a per-attempt suffix. Two attempts on the same job must not
// collide on any pathname they write.
func nonce() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}

// Process runs one job to a durable outcome.
func (p *Pipeline) Process(ctx context.Context, job ledger.Job, attempt int) Outcome {
	log := p.log.With(slog.String("job_id", job.JobID), slog.Int("attempt", attempt))

	// A job accepted under a different naming policy must not be renamed here.
	if job.PolicyVersion != p.cfg.Policy.Identity {
		log.Warn("job was accepted under a different naming policy",
			slog.String("event", "policy_mismatch"),
			slog.String("job_policy", job.PolicyVersion),
			slog.String("process_policy", p.cfg.Policy.Identity))
		return p.hold(ctx, job, jobs.CategoryPolicyMismatch, attempt)
	}

	// A job accepted for a different destination must not be published here.
	// The destination is part of what the job was accepted under: a root-only
	// restart would otherwise silently redirect work that was already
	// registered and, for a reserved job, publish it somewhere its reservation
	// does not cover.
	if job.DestinationRoot != nil && *job.DestinationRoot != p.cfg.Storage.Consume {
		log.Warn("job was accepted for a different destination root",
			slog.String("event", "destination_mismatch"),
			slog.String("category", string(jobs.CategoryDestinationMismatch)))
		return p.hold(ctx, job, jobs.CategoryDestinationMismatch, attempt)
	}

	// An existing receipt means this job is already done.
	if receipt, err := p.led.GetReceipt(ctx, job.JobID); err == nil {
		log.Info("delivery settled against an existing receipt",
			slog.String("event", "delivery_settled"),
			slog.String("outcome", "delivered"),
			slog.Int64("size_bytes", receipt.SizeBytes))
		return settled("delivered", jobs.StateDelivered, "")
	} else if !errors.Is(err, ledger.ErrNotFound) {
		return unsettled(err)
	}

	// Terminal ambiguity stays terminal. BeginDelivery preserves these states
	// precisely so this check can see them.
	if job.State == jobs.StateUncertain {
		log.Warn("redelivery of a job whose publication could not be confirmed; not reprocessing",
			slog.String("event", "delivery_settled"),
			slog.String("outcome", "uncertain"),
			slog.String("category", string(jobs.CategoryPublicationUncertain)))
		return settled("uncertain", jobs.StateUncertain, jobs.CategoryPublicationUncertain)
	}

	// A job found mid-publication is a recovery case, not a fresh one.
	if job.State == jobs.StatePublishing {
		return p.recoverPublishing(ctx, job, attempt, log)
	}

	// The bound is the ledger's own delivery counter, which BeginDelivery
	// increments durably on every delivery. The attempt field carried in the
	// broker message is set by the publisher and does not advance when a
	// consumer returns a delivery, and RabbitMQ's x-delivery-count does not
	// advance on an explicit requeue either, so neither can bound a retry.
	if job.DeliveryAttempts > p.cfg.MaxDeliveryAttempts {
		log.Warn("delivery budget exhausted",
			slog.String("event", "retry_exhausted"),
			slog.Int("attempts", job.DeliveryAttempts),
			slog.Int("max_attempts", p.cfg.MaxDeliveryAttempts))
		return p.hold(ctx, job, jobs.CategoryRetryExhausted, attempt)
	}

	// Roots must still be the directories this process validated.
	if err := p.cfg.Roots.Verify(); err != nil {
		log.Error("a storage root is not the directory it was at startup",
			slog.String("event", "storage_root_changed"),
			slog.String("category", string(jobs.CategoryStorageUnavailable)))
		out, _ := p.holdOr(ctx, job, jobs.CategoryStorageUnavailable, attempt, err)
		return out
	}

	// --- the source, opened once ------------------------------------------
	src, entry, err := storage.Open(job.SourceRoot, job.SourceName, p.cfg.Discovery.Recursive)
	if err != nil {
		cat := jobs.Category(storage.RejectionCategory(err))
		log.Warn("source is not usable",
			slog.String("event", "source_rejected"),
			slog.String("category", string(cat)))
		out, _ := p.holdOr(ctx, job, cat, attempt, err)
		return out
	}
	defer src.Close()

	if !p.sourceMatchesRegistration(job, entry) {
		log.Warn("source changed after discovery",
			slog.String("event", "source_mutated"),
			slog.String("category", string(jobs.CategorySourceMutated)))
		return p.hold(ctx, job, jobs.CategorySourceMutated, attempt)
	}

	// --- the name ---------------------------------------------------------
	res, err := p.cfg.Policy.Normalize(job.SourceName, job.JobID)
	if err != nil {
		cat := jobs.Category(naming.HoldCategory(err))
		log.Warn("the naming policy refused this submission",
			slog.String("event", "normalization_refused"),
			slog.String("category", string(cat)))
		return p.hold(ctx, job, cat, attempt)
	}
	if err := p.led.RecordNormalized(ctx, job.JobID, res.Name, attempt); err != nil {
		return unsettled(err)
	}

	// --- the verified working copy ----------------------------------------
	working, out, ok := p.makeWorkingCopy(ctx, job, src, entry, attempt, log)
	if !ok {
		return out
	}

	// --- reservation and publication --------------------------------------
	return p.publish(ctx, job, res, working, attempt, log)
}

// sourceMatchesRegistration compares the observed source with what discovery
// recorded. Identity is checked as well as size, so a file swapped for another
// of the same length is still detected.
func (p *Pipeline) sourceMatchesRegistration(job ledger.Job, e storage.Entry) bool {
	if job.SizeBytes != nil && *job.SizeBytes != e.Size {
		return false
	}
	if job.SourceInode != nil && *job.SourceInode != int64(e.Inode) {
		return false
	}
	if job.SourceDevice != nil && *job.SourceDevice != int64(e.Device) {
		return false
	}
	// PostgreSQL stores timestamptz at microsecond resolution, so the value
	// read back is a truncation of the nanosecond modification time that was
	// written. Comparing them directly reports every source as mutated.
	if job.SourceModifiedAt != nil &&
		!job.SourceModifiedAt.Truncate(time.Microsecond).Equal(e.ModTime.Truncate(time.Microsecond)) {
		return false
	}
	return true
}

// makeWorkingCopy produces a verified copy and returns a path to content that
// has been checked against the fingerprint discovery recorded.
//
// Concurrency is the difficult part. The canonical name for a job's working
// copy is shared by every attempt, so an attempt must never write to it
// directly: an earlier version copied into it, hashed it, and removed it when
// the hash disagreed, which let one attempt destroy or publish another
// attempt's half-written bytes. Here each attempt copies into a private file,
// verifies THAT file, and only then tries to make it the canonical one with
// link(2). Whoever wins the link owns the canonical copy; whoever loses
// verifies the winner's file and uses it if it is right, and keeps using its
// own verified private copy if it cannot be. Nothing is ever unlinked out from
// under another attempt.
func (p *Pipeline) makeWorkingCopy(ctx context.Context, job ledger.Job, src *os.File, entry storage.Entry, attempt int, log *slog.Logger) (string, Outcome, bool) {
	canonical := filepath.Join(p.cfg.Storage.Staging, job.JobID+".work")

	// A canonical copy that already verifies is reusable as-is.
	if sum, size, err := storage.Fingerprint(canonical); err == nil {
		if job.Fingerprint == nil || bytes.Equal(sum, job.Fingerprint) {
			log.Info("reusing the verified working copy from an earlier attempt",
				slog.String("event", "working_copy_reused"), slog.Int64("size_bytes", size))
			return canonical, Outcome{}, true
		}
		// It does not verify. It is NOT removed: another attempt may be the
		// one that created it, and destroying its bytes is exactly what this
		// function must not do. This attempt makes its own copy instead.
		log.Warn("the canonical working copy does not verify; using an attempt-private copy",
			slog.String("event", "working_copy_unverified"))
	}

	private := filepath.Join(p.cfg.Storage.Staging, tempPrefix+job.JobID+"."+nonce()+".work")
	copied, err := storage.CopyFrom(src, private)
	if err != nil {
		cat := jobs.Category(storage.RejectionCategory(err))
		log.Error("could not make a working copy",
			slog.String("event", "working_copy_failed"),
			slog.String("category", string(cat)))
		out, _ := p.holdOr(ctx, job, cat, attempt, err)
		return "", out, false
	}

	// The bytes that landed must be the bytes discovery fingerprinted.
	if job.Fingerprint != nil && !bytes.Equal(copied.Fingerprint, job.Fingerprint) {
		_ = os.Remove(private)
		log.Warn("the working copy does not match the registered fingerprint",
			slog.String("event", "source_mutated"),
			slog.String("category", string(jobs.CategorySourceMutated)))
		return "", p.hold(ctx, job, jobs.CategorySourceMutated, attempt), false
	}
	// The source must not have changed while it was being copied. The check is
	// against the descriptor that was read, not against the pathname.
	if after, serr := src.Stat(); serr != nil || after.Size() != entry.Size || !after.ModTime().Equal(entry.ModTime) {
		_ = os.Remove(private)
		log.Warn("the source changed while it was being copied",
			slog.String("event", "source_mutated"),
			slog.String("category", string(jobs.CategorySourceMutated)))
		return "", p.hold(ctx, job, jobs.CategorySourceMutated, attempt), false
	}

	// Promote to the canonical name, without ever replacing an existing file.
	existed, lerr := storage.LinkExclusive(private, canonical)
	switch {
	case lerr != nil:
		log.Warn("could not promote the working copy; continuing with the attempt-private copy",
			slog.String("event", "working_copy_not_promoted"),
			slog.String("error_kind", storage.RejectionCategory(lerr)))
		return private, Outcome{}, true
	case existed:
		// Another attempt won. Use its copy if it verifies; otherwise keep
		// this attempt's own verified bytes rather than touching theirs.
		if sum, _, ferr := storage.Fingerprint(canonical); ferr == nil &&
			(job.Fingerprint == nil || bytes.Equal(sum, job.Fingerprint)) {
			_ = os.Remove(private)
			return canonical, Outcome{}, true
		}
		return private, Outcome{}, true
	default:
		// This attempt owns the canonical copy; its private link is redundant.
		_ = os.Remove(private)
		return canonical, Outcome{}, true
	}
}

// publish walks the collision sequence, reserving and linking.
func (p *Pipeline) publish(ctx context.Context, job ledger.Job, res naming.Result, working string, attempt int, log *slog.Logger) Outcome {
	root := p.cfg.Storage.Consume

	for n := 0; n <= p.cfg.Policy.MaxCollisionSuffix; n++ {
		candidate, err := p.cfg.Policy.Candidate(res, n)
		if err != nil {
			return p.hold(ctx, job, jobs.CategoryNameTooLong, attempt)
		}
		// The FINAL name, suffix and shortening included, must satisfy the
		// policy's own invariants. A configured rule can make a suffixed name
		// normalize to something else, which would mean the published name is
		// not a fixed point of the policy that produced it.
		if err := p.cfg.Policy.VerifyFinalName(candidate, job.JobID); err != nil {
			log.Error("the final name does not satisfy the naming invariants",
				slog.String("event", "final_name_invalid"),
				slog.String("category", string(jobs.CategoryPolicyNotIdempotent)))
			return p.hold(ctx, job, jobs.CategoryPolicyNotIdempotent, attempt)
		}
		key := naming.ReservationKey(candidate)

		_, err = p.led.ReserveName(ctx, job.JobID, root, key, candidate, n)
		switch {
		case errors.Is(err, ledger.ErrReservedByAnother):
			p.metrics.CollisionSuffixes.Inc()
			continue
		case err != nil:
			return unsettled(err)
		}

		out, done := p.linkIntoPlace(ctx, job, root, candidate, key, working, attempt, n, log)
		if done {
			return out
		}
	}

	log.Error("the collision sequence was exhausted",
		slog.String("event", "collision_exhausted"),
		slog.Int("max_sequence", p.cfg.Policy.MaxCollisionSuffix))
	return p.hold(ctx, job, jobs.CategoryCollisionExhausted, attempt)
}

// linkIntoPlace performs the intent-link-receipt sequence for one candidate.
func (p *Pipeline) linkIntoPlace(ctx context.Context, job ledger.Job, root, candidate, key, working string, attempt, seq int, log *slog.Logger) (Outcome, bool) {
	final := filepath.Join(root, candidate)
	// Attempt-private: two attempts on one job must not stage over each other.
	tmp := filepath.Join(root, tempPrefix+job.JobID+"."+nonce()+".tmp")

	if err := p.stageBesideDestination(working, tmp); err != nil {
		cat := jobs.Category(storage.RejectionCategory(err))
		log.Error("could not stage the document beside its destination",
			slog.String("event", "staging_failed"), slog.String("category", string(cat)))
		out, _ := p.holdOr(ctx, job, cat, attempt, err)
		return out, true
	}
	defer func() {
		if err := os.Remove(tmp); err != nil && !errors.Is(err, fs.ErrNotExist) {
			log.Warn("could not remove the staged link",
				slog.String("event", "staged_link_left_behind"),
				slog.String("error_kind", "storage_error"))
		}
	}()

	// Intent before action.
	if err := p.led.RecordPublishIntent(ctx, job.JobID, candidate, attempt); err != nil {
		return unsettled(err), true
	}
	p.faults.Fire(config.FaultBeforeLink, log)

	err := storage.PublishExclusive(tmp, final)
	p.faults.Fire(config.FaultAfterLink, log)

	switch {
	case err == nil:
		// The receipt records what is actually at the destination, and it is
		// checked against what this job is supposed to be. Recording a
		// fingerprint without comparing it would let a receipt describe bytes
		// that are not the job's.
		sum, size, ferr := storage.Fingerprint(final)
		if ferr != nil {
			cat := jobs.Category(storage.RejectionCategory(ferr))
			out, _ := p.holdOr(ctx, job, cat, attempt, ferr)
			return out, true
		}
		if job.Fingerprint != nil && !bytes.Equal(sum, job.Fingerprint) {
			log.Error("the published file is not this job's content",
				slog.String("event", "published_content_mismatch"),
				slog.String("category", string(jobs.CategoryDestinationConflict)))
			return p.hold(ctx, job, jobs.CategoryDestinationConflict, attempt), true
		}

		receipt := ledger.Receipt{
			JobID: job.JobID, DestinationRoot: root, DeliveredName: candidate,
			SizeBytes: size, Fingerprint: sum, Attempt: attempt,
		}
		if derr := p.led.RecordDelivered(ctx, receipt, false); derr != nil {
			log.Error("published but could not record the receipt; the delivery will be retried and reconciled",
				slog.String("event", "receipt_write_failed"),
				slog.String("dependency", "postgres_primary"))
			return unsettled(derr), true
		}
		p.faults.Fire(config.FaultAfterReceipt, log)
		p.metrics.Published.Inc()
		p.metrics.PublishedBytes.Add(float64(size))
		log.Info("document published",
			slog.String("event", "document_published"),
			slog.Int("collision_sequence", seq),
			slog.Int64("size_bytes", size))
		return settled("delivered", jobs.StateDelivered, ""), true

	case errors.Is(err, storage.ErrDestinationExists):
		return p.resolveOccupiedDestination(ctx, job, root, candidate, key, final, attempt, log)

	default:
		cat := jobs.Category(storage.RejectionCategory(err))
		log.Error("publication failed",
			slog.String("event", "publication_failed"),
			slog.String("category", string(cat)))
		out, _ := p.holdOr(ctx, job, cat, attempt, err)
		return out, true
	}
}

// resolveOccupiedDestination decides what an existing destination file means.
//
// Matching content is NOT sufficient to claim it. Two distinct submissions may
// legitimately hold identical bytes, and the contract says they stay distinct;
// a file that merely looks like this job's is somebody else's until there is
// evidence this job put it there. The evidence required is this job's own
// publication intent for this exact name -- committed before any link this job
// performs, so it is present for a retry and absent on a first attempt.
func (p *Pipeline) resolveOccupiedDestination(ctx context.Context, job ledger.Job, root, candidate, key, final string, attempt int, log *slog.Logger) (Outcome, bool) {
	sum, size, err := storage.Fingerprint(final)
	if err != nil {
		// Unreadable, a symlink, or not a regular file. It cannot be
		// established as this job's work, and taking the name would risk
		// overwriting a document; retrying would spin against a condition
		// that will not change by itself.
		log.Error("the destination is occupied by something this process cannot verify",
			slog.String("event", "destination_unverifiable"),
			slog.String("category", string(jobs.CategoryDestinationConflict)),
			slog.String("error_kind", storage.RejectionCategory(err)))
		return p.hold(ctx, job, jobs.CategoryDestinationConflict, attempt), true
	}

	contentMatches := job.Fingerprint != nil && bytes.Equal(sum, job.Fingerprint)
	ours := contentMatches &&
		job.PublishAttemptedAt != nil &&
		job.ReservedName != nil && *job.ReservedName == candidate

	if ours {
		receipt := ledger.Receipt{
			JobID: job.JobID, DestinationRoot: root, DeliveredName: candidate,
			SizeBytes: size, Fingerprint: sum, Attempt: attempt,
		}
		if err := p.led.RecordDelivered(ctx, receipt, true); err != nil {
			return unsettled(err), true
		}
		log.Info("reconciled an existing destination as this job's own delivery",
			slog.String("event", "delivery_reconciled"),
			slog.Int64("size_bytes", size))
		return settled("reconciled", jobs.StateDelivered, ""), true
	}

	// Not ours -- even if the bytes match. Keep the reservation blocked so the
	// name is never silently reused, and advance the sequence.
	if err := p.led.BlockReservation(ctx, job.JobID, root, key); err != nil {
		return unsettled(err), true
	}
	p.metrics.CollisionSuffixes.Inc()
	log.Info("destination is occupied by a file this job did not publish; advancing the sequence",
		slog.String("event", "destination_occupied"),
		slog.Bool("content_matches", contentMatches),
		slog.String("category", string(jobs.CategoryDestinationConflict)))
	return Outcome{}, false
}

// recoverPublishing decides the outcome of a job interrupted mid-publication.
func (p *Pipeline) recoverPublishing(ctx context.Context, job ledger.Job, attempt int, log *slog.Logger) Outcome {
	root := p.cfg.Storage.Consume

	name := ""
	if job.ReservedName != nil {
		name = *job.ReservedName
	}
	if name == "" {
		if r, err := p.led.ActiveReservation(ctx, job.JobID, root); err == nil {
			name = r.ReservedName
		}
	}
	if name == "" {
		log.Error("a publication was attempted but no destination name is recorded",
			slog.String("event", "publication_uncertain"),
			slog.String("category", string(jobs.CategoryPublicationUncertain)))
		return p.uncertain(ctx, job, attempt)
	}

	final := filepath.Join(root, name)
	sum, size, err := storage.Fingerprint(final)
	switch {
	case err == nil && job.Fingerprint != nil && bytes.Equal(sum, job.Fingerprint):
		receipt := ledger.Receipt{
			JobID: job.JobID, DestinationRoot: root, DeliveredName: name,
			SizeBytes: size, Fingerprint: sum, Attempt: attempt,
		}
		if err := p.led.RecordDelivered(ctx, receipt, true); err != nil {
			return unsettled(err)
		}
		log.Info("recovered a publication and reconciled it without republishing",
			slog.String("event", "delivery_reconciled"), slog.Int64("size_bytes", size))
		return settled("reconciled", jobs.StateDelivered, "")

	case err == nil:
		log.Error("the reserved destination holds content this job did not publish",
			slog.String("event", "destination_conflict"),
			slog.String("category", string(jobs.CategoryDestinationConflict)))
		return p.hold(ctx, job, jobs.CategoryDestinationConflict, attempt)

	case errors.Is(err, fs.ErrNotExist):
		// The honest answer. The file may never have been created, or it may
		// have been created and already taken by the consumer. A filesystem
		// handoff cannot distinguish those, so the job stops here rather than
		// being redelivered blindly.
		log.Error("a publication may have occurred but cannot be confirmed; not redelivering",
			slog.String("event", "publication_uncertain"),
			slog.String("category", string(jobs.CategoryPublicationUncertain)))
		return p.uncertain(ctx, job, attempt)

	default:
		cat := jobs.Category(storage.RejectionCategory(err))
		out, _ := p.holdOr(ctx, job, cat, attempt, err)
		return out
	}
}

// stageBesideDestination places the bytes next to the destination.
//
// tmp is attempt-private and must not already exist; a hard link is instant
// when staging and consume share a filesystem, and a copy is the fallback when
// they genuinely do not.
func (p *Pipeline) stageBesideDestination(working, tmp string) error {
	if err := os.Link(working, tmp); err == nil {
		return nil
	} else if errors.Is(err, fs.ErrExist) {
		return err
	}
	_, err := storage.CopyVerified(working, tmp)
	return err
}

func (p *Pipeline) hold(ctx context.Context, job ledger.Job, cat jobs.Category, attempt int) Outcome {
	err := p.led.RecordHold(ctx, job.JobID, cat, attempt)
	switch {
	case err == nil:
		return settled("held", jobs.StateHeld, cat)
	case errors.Is(err, ledger.ErrOutcomeAlreadyRecorded):
		// Another attempt reached a durable outcome first and it stands. This
		// attempt settles against it rather than demoting it or retrying.
		p.log.Info("a newer outcome is already recorded for this job; not overwriting it",
			slog.String("event", "outcome_preserved"),
			slog.String("job_id", job.JobID),
			slog.String("category", string(cat)))
		return settled("held", jobs.StateHeld, cat)
	default:
		return unsettled(err)
	}
}

// holdOr records a hold, but keeps a storage failure retryable while the
// durable budget lasts.
func (p *Pipeline) holdOr(ctx context.Context, job ledger.Job, cat jobs.Category, attempt int, cause error) (Outcome, bool) {
	switch cat {
	case jobs.CategoryStorageUnavailable, jobs.CategoryStorageError,
		jobs.CategoryPermissionDenied, jobs.CategorySourceAbsent:
		if job.DeliveryAttempts <= p.cfg.MaxDeliveryAttempts {
			return unsettled(cause), false
		}
	}
	return p.hold(ctx, job, cat, attempt), true
}

func (p *Pipeline) uncertain(ctx context.Context, job ledger.Job, attempt int) Outcome {
	if err := p.led.RecordUncertain(ctx, job.JobID, jobs.CategoryPublicationUncertain, attempt); err != nil {
		return unsettled(err)
	}
	return settled("uncertain", jobs.StateUncertain, jobs.CategoryPublicationUncertain)
}
