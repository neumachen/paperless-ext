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
	"syscall"
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
	// Deferred distinguishes "a sibling is doing this work" from "a dependency
	// failed". Both return the delivery to the broker, but only the second is
	// a fault: a deferral must not be reported as a database problem, must not
	// pause the consumer's other work, and must not spend the job's retry
	// budget on a sibling that is succeeding.
	Deferred bool
	// DeferredTo names the attempt holding the publication claim, for history.
	DeferredTo string
	// Cause names what actually failed when nothing was settled, and
	// Dependency names which one. Empty means the ledger, which is the only
	// thing the requeue path used to be able to say: a destination the kernel
	// refused and a disk with no space were both reported as
	// `postgres_primary` / `ledger_unavailable`, and both detached the
	// consumer, so one job's storage fault stalled every unrelated job the
	// worker held. That is the same mistake a deferral used to make.
	Cause      jobs.Category
	Dependency string
}

func settled(label string, state jobs.State, cat jobs.Category) Outcome {
	return Outcome{Settled: true, Label: label, State: state, Category: cat}
}

func unsettled(err error) Outcome {
	return Outcome{Settled: false, Label: "requeued", Err: err}
}

// retryable is an unsettled outcome whose cause is storage, not the ledger:
// the attempt failed, the failure is retryable while the budget lasts, and
// nothing is wrong with the database.
func retryable(err error, cat jobs.Category) Outcome {
	return Outcome{Settled: false, Label: "requeued", Err: err, Cause: cat, Dependency: "storage"}
}

func deferred(holder string, err error) Outcome {
	return Outcome{Settled: false, Label: "deferred", Err: err, Deferred: true, DeferredTo: holder}
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
	// Work accepted before the destination was recorded is AMBIGUOUS, not
	// compatible with anything. Treating a missing root as "any root will do"
	// meant a job accepted under one configuration could follow a newly
	// configured destination after a restart -- the exact reinterpretation the
	// recorded root exists to prevent, simply unguarded for historical rows.
	// Migration 0004 marks those rows, and they are held for an operator.
	if job.DestinationRootUnknown {
		log.Warn("job was accepted before its destination was recorded; not adopting the current one",
			slog.String("event", "destination_unknown"),
			slog.String("category", string(jobs.CategoryDestinationMismatch)))
		return p.hold(ctx, job, jobs.CategoryDestinationMismatch, attempt)
	}
	if job.DestinationRoot != nil && *job.DestinationRoot != "" &&
		*job.DestinationRoot != p.cfg.Storage.Consume {
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

	// Roots must still be the directories this process validated -- checked
	// BEFORE the recovery branch, which reads and may link inside the consume
	// root. Verifying only on the ordinary path left recovery running against
	// a root that could have been replaced.
	if err := p.cfg.Roots.Verify(); err != nil {
		log.Error("a storage root is not the directory it was at startup",
			slog.String("event", "storage_root_changed"),
			slog.String("category", string(jobs.CategoryStorageUnavailable)))
		out, _ := p.holdOr(ctx, job, jobs.CategoryStorageUnavailable, attempt, err)
		return out
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

// linkIntoPlace performs the claim-link-receipt sequence for one candidate.
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

	// The identity of the bytes about to be linked. After a successful link the
	// destination IS this inode, which is what makes ownership provable later.
	staged, serr := storage.Identify(tmp)
	if serr != nil {
		out, _ := p.holdOr(ctx, job, jobs.Category(storage.RejectionCategory(serr)), attempt, serr)
		return out, true
	}

	// Claim the right to publish. Exclusive per job: a sibling mid-publication
	// makes this attempt stand down rather than race it to a suffixed name.
	claim, cerr := p.led.ClaimPublication(ctx, job.JobID, candidate,
		int64(staged.Inode), int64(staged.Device), attempt, p.cfg.PublishTakeoverAfter)
	switch {
	case errors.Is(cerr, ledger.ErrPublicationInProgress):
		log.Info("another attempt is publishing this job; standing down",
			slog.String("event", "publication_in_progress"),
			slog.String("held_by", ledger.ClaimHolder(cerr)))
		return deferred(ledger.ClaimHolder(cerr), cerr), true
	case errors.Is(cerr, ledger.ErrOutcomeAlreadyRecorded):
		log.Info("a durable outcome was recorded while this attempt was preparing",
			slog.String("event", "outcome_preserved"))
		return settled("delivered", jobs.StateDelivered, ""), true
	case cerr != nil:
		return unsettled(cerr), true
	}
	_ = claim

	// Armed only in the interruption exercise: stop with the claim committed
	// and the destination untouched.
	//
	// This call was lost when linkIntoPlace was rewritten around the exclusive
	// claim, which left `before_link` accepted by the configuration, named in
	// the startup warning, and completely inert -- so the scenario that
	// depends on it published normally and the oracle read the result as a
	// product failure. An armed fault point that does nothing is worse than
	// one that does not exist.
	p.faults.Fire(config.FaultBeforeLink, log)

	// Hold the claim open, if armed, BEFORE the link. This is the window the
	// duplicate-publication race lives in: a second attempt arriving here sees
	// no file at the destination, so the claim is the only thing that can stop
	// it from publishing a second copy. Pausing after the link would let the
	// occupied destination do the work instead and would prove the weaker
	// property.
	p.faults.Pause(config.FaultHoldAfterClaim, log)

	err := storage.PublishExclusive(tmp, final)
	if err == nil {
		// Fired only when the link actually succeeded, so `after_link` means
		// what its name says. An EEXIST interruption is a different scenario
		// and would need its own point.
		p.faults.Fire(config.FaultAfterLink, log)
		// Held open, if armed, so a real consumer can take the document before
		// the read below. The publication has already happened at this point;
		// what follows must not be able to undo that.
		p.faults.Pause(config.FaultHoldAfterLink, log)
	}

	switch {
	case err == nil:
		// The link returned success, so publication HAPPENED. What is at the
		// destination now is a separate question: a consumer may already have
		// taken it. Reading the destination is therefore best-effort evidence,
		// never the thing that decides whether we published.
		size, fp := int64(0), job.Fingerprint
		if job.SizeBytes != nil {
			size = *job.SizeBytes
		}
		absent := false
		switch sum, n, ferr := storage.Fingerprint(final); {
		case ferr == nil:
			if job.Fingerprint != nil && !bytes.Equal(sum, job.Fingerprint) {
				log.Error("the published file is not this job's content",
					slog.String("event", "published_content_mismatch"),
					slog.String("category", string(jobs.CategoryDestinationConflict)))
				return p.hold(ctx, job, jobs.CategoryDestinationConflict, attempt), true
			}
			fp, size = sum, n
		case errors.Is(ferr, fs.ErrNotExist):
			// Gone between the link and the read. The old code ran this
			// through the generic storage classifier and held the job as
			// source_absent -- reporting a missing SOURCE for a document that
			// had just been published successfully, and losing the delivery
			// from the accounting entirely.
			absent = true
			log.Info("the destination was removed immediately after publication; recording the delivery",
				slog.String("event", "destination_consumed_immediately"))
		default:
			cat := jobs.Category(storage.RejectionCategory(ferr))
			out, _ := p.holdOr(ctx, job, cat, attempt, ferr)
			return out, true
		}

		receipt := ledger.Receipt{
			JobID: job.JobID, DestinationRoot: root, DeliveredName: candidate,
			SizeBytes: size, Fingerprint: fp, Attempt: attempt,
		}
		// The link already succeeded, so this receipt records something that
		// has happened. It is written even if the attempt's budget has just
		// run out; otherwise a completed publication is reconciled later as a
		// recovery instead of being recorded now.
		rctx, rcancel := durably(ctx)
		defer rcancel()
		if derr := p.led.RecordDelivered(rctx, receipt, false); derr != nil {
			log.Error("published but could not record the receipt; the delivery will be retried and reconciled",
				slog.String("event", "receipt_write_failed"),
				slog.String("dependency", "postgres_primary"))
			return unsettled(derr), true
		}
		if absent {
			if nerr := p.led.NoteDeliveredFileAbsent(rctx, job.JobID); nerr != nil {
				log.Warn("could not record that the delivered file was already absent",
					slog.String("event", "absence_note_failed"))
			}
		}
		p.faults.Fire(config.FaultAfterReceipt, log)
		p.metrics.Published.Inc()
		p.metrics.PublishedBytes.Add(float64(size))
		log.Info("document published",
			slog.String("event", "document_published"),
			slog.Int("collision_sequence", seq),
			slog.Bool("destination_already_absent", absent),
			slog.Int64("size_bytes", size))
		return settled("delivered", jobs.StateDelivered, ""), true

	case errors.Is(err, storage.ErrDestinationExists):
		return p.resolveOccupiedDestination(ctx, job, root, candidate, key, final, attempt, log)

	default:
		cat := jobs.Category(storage.RejectionCategory(err))
		log.Error("publication failed",
			slog.String("event", "publication_failed"),
			slog.String("category", string(cat)))

		// link(2) is atomic: a refusal means no directory entry was created,
		// so nothing was published and the claim should not stand. Leaving it
		// standing left the job in `publishing`, which is the recovery path's
		// input -- so the next delivery did not retry the write, it "recovered"
		// it, found no destination, and recorded `uncertain`. A destination the
		// kernel had plainly refused became an unresolvable outcome instead of
		// a retry that would have recorded the real reason.
		//
		// Only definite refusals qualify. An ambiguous failure must keep the
		// claim, because then a publication may in fact have happened.
		if definitelyNotPublished(err) {
			actx, acancel := durably(ctx)
			aerr := p.led.AbandonPublication(actx, job.JobID, attempt, string(cat))
			acancel()
			if aerr != nil {
				log.Warn("could not withdraw the publication claim after a refused link",
					slog.String("event", "publish_abandon_failed"),
					slog.String("error_kind", storage.RejectionCategory(aerr)))
			}
		}

		out, _ := p.holdOr(ctx, job, cat, attempt, err)
		return out, true
	}
}

// definitelyNotPublished reports whether a link failure proves no directory
// entry was created.
//
// The listed errors are refusals the kernel makes BEFORE creating anything:
// permission, a read-only filesystem, no space, a name the filesystem will not
// take, a missing or non-directory parent. Anything else -- an I/O error most
// of all -- is ambiguous, and an ambiguous publication must stay ambiguous.
func definitelyNotPublished(err error) bool {
	switch {
	case errors.Is(err, fs.ErrPermission),
		errors.Is(err, syscall.EROFS),
		errors.Is(err, syscall.ENOSPC),
		errors.Is(err, syscall.EDQUOT),
		errors.Is(err, syscall.ENAMETOOLONG),
		errors.Is(err, syscall.ENOTDIR),
		errors.Is(err, syscall.EMLINK),
		errors.Is(err, syscall.EXDEV):
		return true
	}
	return false
}

// resolveOccupiedDestination decides what an existing destination file means.
//
// Ownership is IDENTITY, not content. Two distinct submissions may legitimately
// hold identical bytes and the contract says they stay distinct, so a file that
// merely looks like this job's is somebody else's until the inode says
// otherwise. The inode compared against is the one recorded when this job took
// its publication claim, which is the inode it linked.
//
// The job row is re-read here rather than taken from the snapshot loaded at
// delivery time. That snapshot is why overlapping attempts could duplicate a
// publication: the second attempt's copy predated the first attempt's claim, so
// it saw no publication for this job and concluded the first attempt's document
// was foreign.
func (p *Pipeline) resolveOccupiedDestination(ctx context.Context, job ledger.Job, root, candidate, key, final string, attempt int, log *slog.Logger) (Outcome, bool) {
	fresh, ferr := p.led.ReadJob(ctx, job.JobID)
	if ferr != nil {
		return unsettled(ferr), true
	}
	if fresh.State == jobs.StateDelivered {
		log.Info("a sibling attempt completed this job while this one was publishing",
			slog.String("event", "outcome_preserved"))
		return settled("delivered", jobs.StateDelivered, ""), true
	}

	got, gerr := storage.Identify(final)
	if gerr != nil {
		log.Error("the destination is occupied by something this process cannot verify",
			slog.String("event", "destination_unverifiable"),
			slog.String("category", string(jobs.CategoryDestinationConflict)),
			slog.String("error_kind", storage.RejectionCategory(gerr)))
		return p.hold(ctx, job, jobs.CategoryDestinationConflict, attempt), true
	}

	ours := fresh.PublishInode != nil && fresh.PublishDevice != nil &&
		*fresh.PublishInode == int64(got.Inode) && *fresh.PublishDevice == int64(got.Device)

	if ours {
		sum, size, serr := storage.Fingerprint(final)
		if serr != nil {
			out, _ := p.holdOr(ctx, job, jobs.Category(storage.RejectionCategory(serr)), attempt, serr)
			return out, true
		}
		if job.Fingerprint != nil && !bytes.Equal(sum, job.Fingerprint) {
			// Our inode, wrong bytes: something rewrote it in place.
			return p.hold(ctx, job, jobs.CategoryDestinationConflict, attempt), true
		}
		receipt := ledger.Receipt{
			JobID: job.JobID, DestinationRoot: root, DeliveredName: candidate,
			SizeBytes: size, Fingerprint: sum, Attempt: attempt,
		}
		// Reconciling records a delivery that already exists on disk; the same
		// grace applies as for a fresh receipt.
		rctx, rcancel := durably(ctx)
		defer rcancel()
		if err := p.led.RecordDelivered(rctx, receipt, true); err != nil {
			return unsettled(err), true
		}
		log.Info("reconciled an existing destination as this job's own delivery",
			slog.String("event", "delivery_reconciled"), slog.Int64("size_bytes", size))
		return settled("reconciled", jobs.StateDelivered, ""), true
	}

	// Not ours. Whether the bytes match is recorded but decides nothing: equal
	// content is exactly the case the identity check exists for, and treating
	// it as ownership would be implicit deduplication.
	matches := false
	if sum, _, serr := storage.Fingerprint(final); serr == nil && job.Fingerprint != nil {
		matches = bytes.Equal(sum, job.Fingerprint)
	}
	if err := p.led.BlockReservation(ctx, job.JobID, root, key); err != nil {
		return unsettled(err), true
	}
	p.metrics.CollisionSuffixes.Inc()
	log.Info("destination is occupied by a file this job did not publish; advancing the sequence",
		slog.String("event", "destination_occupied"),
		slog.Bool("content_matches", matches),
		slog.String("category", string(jobs.CategoryDestinationConflict)))
	return Outcome{}, false
}

// recoverPublishing decides the outcome of a job interrupted mid-publication.
//
// It runs after the root verification in Process, so a recovery cannot read or
// write through a root that is no longer the directory it was.
func (p *Pipeline) recoverPublishing(ctx context.Context, job ledger.Job, attempt int, log *slog.Logger) Outcome {
	root := p.cfg.Storage.Consume

	// `publishing` does not mean "abandoned". It is also exactly what a job
	// looks like while a LIVE attempt sits between its claim and its link.
	//
	// Recovery used to skip this check, so a sibling handed the same job --
	// which is what happens the moment the holder's broker connection drops --
	// looked at the destination, found nothing there yet, and recorded
	// `uncertain` for a publication that was still in progress. The holder
	// then went on to link its document, leaving a file in the consume
	// directory with no receipt and a job whose durable state contradicted
	// what was on disk. That is precisely the conflicting durable identity
	// the claim exists to prevent, arrived at from the other direction.
	//
	// A claim still inside its takeover window therefore defers. Only once it
	// is stale -- the holder is presumed gone -- does recovery decide anything.
	if holder, fresh, herr := p.led.PublicationClaimState(ctx, job.JobID, p.cfg.PublishTakeoverAfter); herr == nil {
		if fresh && holder != "" && holder != p.led.Actor() {
			log.Info("another attempt is still publishing this job; not recovering it",
				slog.String("event", "publication_in_progress"),
				slog.String("held_by", holder))
			return deferred(holder, ledger.ErrPublicationInProgress)
		}
	}

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
	got, gerr := storage.Identify(final)
	switch {
	case gerr == nil:
		// Identity first. A foreign file whose bytes matched this job's was
		// previously adopted here, which both took someone else's file and
		// deduplicated two submissions that must stay distinct.
		ours := job.PublishInode != nil && job.PublishDevice != nil &&
			*job.PublishInode == int64(got.Inode) && *job.PublishDevice == int64(got.Device)
		if !ours {
			log.Error("the reserved destination holds a file this job did not link",
				slog.String("event", "destination_conflict"),
				slog.String("category", string(jobs.CategoryDestinationConflict)))
			return p.hold(ctx, job, jobs.CategoryDestinationConflict, attempt)
		}
		sum, size, serr := storage.Fingerprint(final)
		if serr != nil {
			out, _ := p.holdOr(ctx, job, jobs.Category(storage.RejectionCategory(serr)), attempt, serr)
			return out
		}
		if job.Fingerprint != nil && !bytes.Equal(sum, job.Fingerprint) {
			return p.hold(ctx, job, jobs.CategoryDestinationConflict, attempt)
		}
		receipt := ledger.Receipt{
			JobID: job.JobID, DestinationRoot: root, DeliveredName: name,
			SizeBytes: size, Fingerprint: sum, Attempt: attempt,
		}
		rctx, rcancel := durably(ctx)
		defer rcancel()
		if err := p.led.RecordDelivered(rctx, receipt, true); err != nil {
			return unsettled(err)
		}
		log.Info("recovered a publication and reconciled it without republishing",
			slog.String("event", "delivery_reconciled"), slog.Int64("size_bytes", size))
		return settled("reconciled", jobs.StateDelivered, "")

	case errors.Is(gerr, fs.ErrNotExist):
		// The honest answer. The file may never have been created, or it may
		// have been created and already taken by the consumer. A filesystem
		// handoff cannot distinguish those, so the job stops here rather than
		// being redelivered blindly.
		log.Error("a publication may have occurred but cannot be confirmed; not redelivering",
			slog.String("event", "publication_uncertain"),
			slog.String("category", string(jobs.CategoryPublicationUncertain)))
		return p.uncertain(ctx, job, attempt)

	default:
		cat := jobs.Category(storage.RejectionCategory(gerr))
		out, _ := p.holdOr(ctx, job, cat, attempt, gerr)
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

// recordGrace bounds a durable write that has outlived the attempt that
// established what it records.
const recordGrace = 10 * time.Second

// durably returns a context for recording an outcome this attempt has ALREADY
// determined.
//
// The handler budget bounds the ATTEMPT, which is right: an attempt cannot run
// forever. But an outcome established just before the budget ran out is a
// fact, and dropping it is not a neutral failure. The job stays in
// `publishing`, the next delivery reads that as "a publication may have
// happened", finds no destination, and settles `uncertain` -- so a publication
// the kernel had plainly refused becomes an unresolvable outcome needing a
// person. Losing a known result is strictly worse than spending another second
// writing it down.
//
// So a write that records something already decided is detached from the
// attempt's deadline and bounded on its own. This is the same reasoning the
// consumer already applies when it detaches a running handler from shutdown.
// While the attempt's context is still live, nothing changes.
func durably(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx.Err() == nil {
		return ctx, func() {}
	}
	return context.WithTimeout(context.WithoutCancel(ctx), recordGrace)
}

func (p *Pipeline) hold(ctx context.Context, job ledger.Job, cat jobs.Category, attempt int) Outcome {
	ctx, cancel := durably(ctx)
	defer cancel()
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
	// Deliberately NOT including source_absent. A vanished source is almost
	// always permanent, and retrying it spends the budget only to end in
	// retry_exhausted -- which replaces an informative reason with a generic
	// one. It is held immediately, with the reason intact.
	case jobs.CategoryStorageUnavailable, jobs.CategoryStorageError,
		jobs.CategoryPermissionDenied:
		// `<` and not `<=`, so the LAST permitted attempt records the real
		// reason instead of returning the delivery one more time.
		//
		// With `<=`, the final delivery was requeued and the one after it was
		// stopped by Process's budget check before it ever reached storage --
		// so a destination the kernel refuses to write, a root that is gone,
		// and a disk that is full all ended up recorded as `retry_exhausted`.
		// That is the same loss of an informative reason the source_absent
		// note above exists to prevent, and it made every persistent storage
		// fault look identical to an operator.
		if job.DeliveryAttempts < p.cfg.MaxDeliveryAttempts {
			return retryable(cause, cat), false
		}
	}
	return p.hold(ctx, job, cat, attempt), true
}

func (p *Pipeline) uncertain(ctx context.Context, job ledger.Job, attempt int) Outcome {
	ctx, cancel := durably(ctx)
	defer cancel()
	if err := p.led.RecordUncertain(ctx, job.JobID, jobs.CategoryPublicationUncertain, attempt); err != nil {
		return unsettled(err)
	}
	return settled("uncertain", jobs.StateUncertain, jobs.CategoryPublicationUncertain)
}
