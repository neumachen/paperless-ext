package renamer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"

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
// That last case is the unresolved publication/receipt/consumption window the
// contract requires to stay visible instead of being guessed.
type Pipeline struct {
	cfg     config.RenamerConfig
	led     *ledger.Ledger
	log     *slog.Logger
	metrics *telemetry.Metrics
}

// NewPipeline builds the processing pipeline.
func NewPipeline(cfg config.RenamerConfig, led *ledger.Ledger, log *slog.Logger, m *telemetry.Metrics) *Pipeline {
	return &Pipeline{cfg: cfg, led: led, log: log, metrics: m}
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

// tempPrefix marks the short-lived link target inside the consume directory.
//
// It is a dotfile because the consume directory is watched by Paperless, and
// the conventional signal for "not a submission" there is a leading dot. The
// prefix only has to survive for the duration of one link call: the file that
// ever becomes visible under a real name is created by link(2) and is complete
// at the instant it appears.
const tempPrefix = ".fn-"

// Process runs one job to a durable outcome.
func (p *Pipeline) Process(ctx context.Context, job ledger.Job, attempt int) Outcome {
	log := p.log.With(slog.String("job_id", job.JobID), slog.Int("attempt", attempt))

	// A job accepted under a different naming policy must not be renamed here.
	// Silently applying this process's rules is exactly the reinterpretation a
	// rolling restart must not perform.
	if job.PolicyVersion != p.cfg.Policy.Identity {
		log.Warn("job was accepted under a different naming policy",
			slog.String("event", "policy_mismatch"),
			slog.String("job_policy", job.PolicyVersion),
			slog.String("process_policy", p.cfg.Policy.Identity))
		return p.hold(ctx, job, jobs.CategoryPolicyMismatch, attempt)
	}

	// An existing receipt means this job is already done. A redelivery of a
	// delivered job is settled without touching anything.
	if receipt, err := p.led.GetReceipt(ctx, job.JobID); err == nil {
		log.Info("delivery settled against an existing receipt",
			slog.String("event", "delivery_settled"),
			slog.String("outcome", "delivered"),
			slog.Int64("size_bytes", receipt.SizeBytes))
		return settled("delivered", jobs.StateDelivered, "")
	} else if !errors.Is(err, ledger.ErrNotFound) {
		return unsettled(err)
	}

	// A job found mid-publication is a recovery case, not a fresh one.
	if job.State == jobs.StatePublishing {
		return p.recoverPublishing(ctx, job, attempt, log)
	}

	if attempt > p.cfg.MaxDeliveryAttempts {
		log.Warn("delivery budget exhausted",
			slog.String("event", "retry_exhausted"),
			slog.Int("max_attempts", p.cfg.MaxDeliveryAttempts))
		return p.hold(ctx, job, jobs.CategoryRetryExhausted, attempt)
	}

	// --- the source -------------------------------------------------------
	entry, err := storage.Inspect(job.SourceRoot, job.SourceName)
	if err != nil {
		cat := jobs.Category(storage.RejectionCategory(err))
		log.Warn("source is not usable",
			slog.String("event", "source_rejected"),
			slog.String("category", string(cat)))
		return p.hold(ctx, job, cat, attempt)
	}
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

	// --- dry run ----------------------------------------------------------
	// Nothing below this point runs in a dry run: no working copy, no
	// reservation, no publication, and no change to the job's state.
	if p.cfg.DryRun {
		log.Info("dry run examined a job without acting",
			slog.String("event", "dry_run"),
			slog.Bool("used_fallback", res.UsedFallback),
			slog.Bool("shortened", res.Shortened))
		p.metrics.Deliveries.WithLabelValues("dry_run").Inc()
		return Outcome{Settled: true, Label: "dry_run", State: job.State, Category: jobs.CategoryDryRun}
	}

	// --- the verified working copy ----------------------------------------
	working, out, ok := p.makeWorkingCopy(ctx, job, entry, attempt, log)
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
	if job.SourceModifiedAt != nil && !job.SourceModifiedAt.Equal(e.ModTime) {
		return false
	}
	return true
}

// makeWorkingCopy produces a verified copy in the staging root.
//
// The copy is verified against the fingerprint discovery recorded, so a source
// that changed between discovery and now is caught before anything is
// published. The working copy is retained afterwards; nothing in this build
// deletes it, which is a deliberate storage cost documented in the README.
func (p *Pipeline) makeWorkingCopy(ctx context.Context, job ledger.Job, entry storage.Entry, attempt int, log *slog.Logger) (string, Outcome, bool) {
	dst := filepath.Join(p.cfg.Storage.Staging, job.JobID+".work")

	if existing, size, err := storage.Fingerprint(dst); err == nil {
		// A working copy from an earlier attempt. It is this job's own file,
		// named by its job id, so reusing it after verification is safe and
		// avoids re-reading the source.
		if job.Fingerprint == nil || bytes.Equal(existing, job.Fingerprint) {
			log.Info("reusing the verified working copy from an earlier attempt",
				slog.String("event", "working_copy_reused"), slog.Int64("size_bytes", size))
			return dst, Outcome{}, true
		}
		// It does not match: it cannot be trusted, and it is ours to replace.
		if err := os.Remove(dst); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return "", unsettled(fmt.Errorf("remove an unusable working copy: %w", err)), false
		}
	}

	copied, err := storage.CopyVerified(entry.Path, dst)
	if err != nil {
		cat := jobs.Category(storage.RejectionCategory(err))
		log.Error("could not make a working copy",
			slog.String("event", "working_copy_failed"),
			slog.String("category", string(cat)),
			slog.String("error_kind", cat.String()))
		out, _ := p.holdOr(ctx, job, cat, attempt, err)
		return "", out, false
	}

	// The bytes that landed must be the bytes discovery fingerprinted.
	if job.Fingerprint != nil && !bytes.Equal(copied.Fingerprint, job.Fingerprint) {
		_ = os.Remove(dst)
		log.Warn("the working copy does not match the registered fingerprint",
			slog.String("event", "source_mutated"),
			slog.String("category", string(jobs.CategorySourceMutated)))
		out := p.hold(ctx, job, jobs.CategorySourceMutated, attempt)
		return "", out, false
	}
	// Re-inspect the source: a change during the copy is still a change.
	if after, err := storage.Inspect(job.SourceRoot, job.SourceName); err != nil || !storage.SameFile(entry, after) {
		_ = os.Remove(dst)
		log.Warn("the source changed while it was being copied",
			slog.String("event", "source_mutated"),
			slog.String("category", string(jobs.CategorySourceMutated)))
		out := p.hold(ctx, job, jobs.CategorySourceMutated, attempt)
		return "", out, false
	}
	return dst, Outcome{}, true
}

// publish walks the collision sequence, reserving and linking.
func (p *Pipeline) publish(ctx context.Context, job ledger.Job, res naming.Result, working string, attempt int, log *slog.Logger) Outcome {
	root := p.cfg.Storage.Consume

	for n := 0; n <= p.cfg.Policy.MaxCollisionSuffix; n++ {
		candidate, err := p.cfg.Policy.Candidate(res, n)
		if err != nil {
			return p.hold(ctx, job, jobs.CategoryNameTooLong, attempt)
		}
		key := naming.ReservationKey(candidate)

		_, err = p.led.ReserveName(ctx, job.JobID, root, key, candidate, n)
		switch {
		case errors.Is(err, ledger.ErrReservedByAnother):
			// Another submission owns this name, or this job already found it
			// occupied. Either way the sequence advances; nothing is reused.
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
//
// done is false only when the candidate turned out to be occupied by someone
// else's file and the caller should advance the sequence.
func (p *Pipeline) linkIntoPlace(ctx context.Context, job ledger.Job, root, candidate, key, working string, attempt, seq int, log *slog.Logger) (Outcome, bool) {
	final := filepath.Join(root, candidate)
	tmp := filepath.Join(root, tempPrefix+job.JobID+".tmp")

	// Stage the bytes inside the destination directory so the publication step
	// itself never crosses a filesystem boundary. A hard link is tried first
	// and costs nothing when staging and consume share a filesystem; a copy is
	// the fallback when they genuinely do not.
	if err := p.stageBesideDestination(working, tmp); err != nil {
		cat := jobs.Category(storage.RejectionCategory(err))
		log.Error("could not stage the document beside its destination",
			slog.String("event", "staging_failed"), slog.String("category", string(cat)))
		out, _ := p.holdOr(ctx, job, cat, attempt, err)
		return out, true
	}
	defer func() {
		// PublishExclusive removes tmp on success; this covers every other path.
		if err := os.Remove(tmp); err != nil && !errors.Is(err, fs.ErrNotExist) {
			log.Warn("could not remove the staged link",
				slog.String("event", "staged_link_left_behind"),
				slog.String("error_kind", "storage_error"))
		}
	}()

	// Intent before action. After this commit, a crash is recoverable as
	// "may have published" rather than being indistinguishable from "never
	// started".
	if err := p.led.RecordPublishIntent(ctx, job.JobID, candidate, attempt); err != nil {
		return unsettled(err), true
	}

	err := storage.PublishExclusive(tmp, final)
	switch {
	case err == nil:
		size, fp := int64(0), []byte(nil)
		if sum, n, ferr := storage.Fingerprint(final); ferr == nil {
			fp, size = sum, n
		} else if job.Fingerprint != nil {
			fp = job.Fingerprint
			if job.SizeBytes != nil {
				size = *job.SizeBytes
			}
		}
		receipt := ledger.Receipt{
			JobID: job.JobID, DestinationRoot: root, DeliveredName: candidate,
			SizeBytes: size, Fingerprint: fp, Attempt: attempt,
		}
		if err := p.led.RecordDelivered(ctx, receipt, false); err != nil {
			// Published, but the receipt did not commit. Returning the
			// delivery is correct: the redelivery will find the destination
			// present with this job's content and reconcile it.
			log.Error("published but could not record the receipt; the delivery will be retried and reconciled",
				slog.String("event", "receipt_write_failed"),
				slog.String("dependency", "postgres_primary"))
			return unsettled(err), true
		}
		p.metrics.Published.Inc()
		p.metrics.PublishedBytes.Add(float64(size))
		log.Info("document published",
			slog.String("event", "document_published"),
			slog.Int("collision_sequence", seq),
			slog.Int64("size_bytes", size))
		return settled("delivered", jobs.StateDelivered, ""), true

	case errors.Is(err, storage.ErrDestinationExists):
		// Somebody got there first. Whose file is it?
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
func (p *Pipeline) resolveOccupiedDestination(ctx context.Context, job ledger.Job, root, candidate, key, final string, attempt int, log *slog.Logger) (Outcome, bool) {
	sum, size, err := storage.Fingerprint(final)
	if err != nil {
		cat := jobs.Category(storage.RejectionCategory(err))
		out, _ := p.holdOr(ctx, job, cat, attempt, err)
		return out, true
	}

	if job.Fingerprint != nil && bytes.Equal(sum, job.Fingerprint) {
		// This job's own work, from an attempt whose receipt did not commit.
		// Reconciling is the safe outcome: the document is already in place
		// and must not be published twice or overwritten.
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

	// Someone else's file. Keep the reservation as blocked so the name is
	// never silently reused, and advance the sequence.
	if err := p.led.BlockReservation(ctx, job.JobID, root, key); err != nil {
		return unsettled(err), true
	}
	p.metrics.CollisionSuffixes.Inc()
	log.Info("destination is occupied by content this job did not publish; advancing the sequence",
		slog.String("event", "destination_occupied"),
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
		// Intent recorded but no name survived. Nothing can be established.
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
		// Present, but not this job's content. Nothing is overwritten.
		log.Error("the reserved destination holds content this job did not publish",
			slog.String("event", "destination_conflict"),
			slog.String("category", string(jobs.CategoryDestinationConflict)))
		return p.hold(ctx, job, jobs.CategoryDestinationConflict, attempt)

	case errors.Is(err, fs.ErrNotExist):
		// The honest answer. The file may never have been created, or it may
		// have been created and already consumed by Paperless. A filesystem
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
func (p *Pipeline) stageBesideDestination(working, tmp string) error {
	if err := os.Remove(tmp); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	// Same filesystem: a hard link is instant and copies nothing.
	if err := os.Link(working, tmp); err == nil {
		return nil
	}
	// Genuinely separate filesystems, or a filesystem without hard links.
	_, err := storage.CopyVerified(working, tmp)
	return err
}

func (p *Pipeline) hold(ctx context.Context, job ledger.Job, cat jobs.Category, attempt int) Outcome {
	if err := p.led.RecordHold(ctx, job.JobID, cat, attempt); err != nil {
		return unsettled(err)
	}
	return settled("held", jobs.StateHeld, cat)
}

// holdOr records a hold, but keeps a storage failure retryable.
//
// A hold is for a submission that cannot be processed. A root that is
// unavailable right now is a different thing: the delivery goes back so the
// job can be retried once storage returns, instead of being parked forever.
func (p *Pipeline) holdOr(ctx context.Context, job ledger.Job, cat jobs.Category, attempt int, cause error) (Outcome, bool) {
	switch cat {
	case jobs.CategoryStorageUnavailable, jobs.CategoryStorageError, jobs.CategoryPermissionDenied:
		if attempt <= p.cfg.MaxDeliveryAttempts {
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
