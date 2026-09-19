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
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/logging"
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

	// An existing receipt means the publication is AUTHORISED. It no longer
	// means it is finished.
	//
	// Publication commits the receipt while the document is still an invisible
	// dotfile and reveals it afterwards, so a receipt with the job still in
	// `publishing` describes a publication that was interrupted between those
	// two steps: present, authorised, and not yet visible to the consumer.
	// Acking here left that document hidden forever -- the job stayed
	// `publishing`, every redelivery settled against the receipt, and nothing
	// ever finished it.
	if receipt, err := p.led.GetReceipt(ctx, job.JobID); err == nil {
		if job.State == jobs.StatePublishing {
			log.Info("a committed publication was never revealed; finishing it",
				slog.String("event", "publication_reveal_pending"))
			return p.recoverPublishing(ctx, job, attempt, log)
		}
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
	defer func() { _ = working.Close() }()

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
func (p *Pipeline) makeWorkingCopy(ctx context.Context, job ledger.Job, src *os.File, entry storage.Entry, attempt int, log *slog.Logger) (*os.File, Outcome, bool) {
	// One verified staging directory for every operation below.
	//
	// This path used to work entirely in pathnames: it hashed the canonical
	// copy by name, created the private copy by name, promoted it by name, and
	// removed it by name -- all after a root validation that happened somewhere
	// else, earlier. A staging root repointed in between redirected every one
	// of those, which is the same gap that was closed on the consume side and
	// was still open here.
	dir, derr := storage.OpenDir(p.cfg.Storage.Staging)
	if derr != nil {
		cat := jobs.Category(storage.RejectionCategory(derr))
		log.Error("the staging root could not be opened",
			slog.String("event", "staging_root_unusable"),
			slog.String("category", string(cat)))
		out, _ := p.holdOr(ctx, job, cat, attempt, derr)
		return nil, out, false
	}
	defer func() { _ = dir.Close() }()

	canonical := job.JobID + ".work"

	// A canonical copy that already verifies is reusable as-is, and it is read
	// through the descriptor that answered for it.
	if f, _, sum, size, err := dir.OpenAndFingerprint(canonical); err == nil {
		if job.Fingerprint == nil || bytes.Equal(sum, job.Fingerprint) {
			log.Info("reusing the verified working copy from an earlier attempt",
				slog.String("event", "working_copy_reused"), slog.Int64("size_bytes", size))
			return f, Outcome{}, true
		}
		_ = f.Close()
		// It does not verify. It is NOT removed: another attempt may be the
		// one that created it, and destroying its bytes is exactly what this
		// function must not do. This attempt makes its own copy instead.
		log.Warn("the canonical working copy does not verify; using an attempt-private copy",
			slog.String("event", "working_copy_unverified"))
	}

	privateName := tempPrefix + job.JobID + "." + nonce() + ".work"
	pf, privateID, sum, _, err := dir.CreateFrom(src, privateName, 0o640)
	if err != nil {
		cat := jobs.Category(storage.RejectionCategory(err))
		log.Error("could not make a working copy",
			slog.String("event", "working_copy_failed"),
			slog.String("category", string(cat)))
		out, _ := p.holdOr(ctx, job, cat, attempt, err)
		return nil, out, false
	}

	discard := func() {
		_ = pf.Close()
		if _, rerr := dir.RemoveOwned(privateName, privateID); rerr != nil {
			log.Warn("could not remove this attempt's private working copy",
				slog.String("event", "working_copy_left_behind"),
				slog.String("error_kind", storage.RejectionCategory(rerr)))
		}
	}

	// The bytes that landed must be the bytes discovery fingerprinted.
	if job.Fingerprint != nil && !bytes.Equal(sum, job.Fingerprint) {
		discard()
		log.Warn("the working copy does not match the registered fingerprint",
			slog.String("event", "source_mutated"),
			slog.String("category", string(jobs.CategorySourceMutated)))
		return nil, p.hold(ctx, job, jobs.CategorySourceMutated, attempt), false
	}
	// The source must not have changed while it was being copied. The check is
	// against the descriptor that was read, not against the pathname.
	if after, serr := src.Stat(); serr != nil || after.Size() != entry.Size || !after.ModTime().Equal(entry.ModTime) {
		discard()
		log.Warn("the source changed while it was being copied",
			slog.String("event", "source_mutated"),
			slog.String("category", string(jobs.CategorySourceMutated)))
		return nil, p.hold(ctx, job, jobs.CategorySourceMutated, attempt), false
	}

	// Promote to the canonical name, without ever replacing an existing file.
	existed, lerr := dir.LinkAt(privateName, canonical)
	switch {
	case lerr != nil:
		log.Warn("could not promote the working copy; continuing with the attempt-private copy",
			slog.String("event", "working_copy_not_promoted"),
			slog.String("error_kind", storage.RejectionCategory(lerr)))
		return pf, Outcome{}, true
	case existed:
		// Another attempt won. Use its copy if it verifies; otherwise keep
		// this attempt's own verified bytes rather than touching theirs.
		if cf, _, csum, _, cerr := dir.OpenAndFingerprint(canonical); cerr == nil {
			if job.Fingerprint == nil || bytes.Equal(csum, job.Fingerprint) {
				discard()
				return cf, Outcome{}, true
			}
			_ = cf.Close()
		}
		return pf, Outcome{}, true
	default:
		// This attempt owns the canonical copy. Its private link is redundant;
		// the descriptor stays open and now refers to the canonical inode.
		if _, rerr := dir.RemoveOwned(privateName, privateID); rerr != nil {
			log.Warn("could not remove this attempt's redundant private link",
				slog.String("event", "working_copy_left_behind"),
				slog.String("error_kind", storage.RejectionCategory(rerr)))
		}
		return pf, Outcome{}, true
	}
}

// publish walks the collision sequence, reserving and linking.
func (p *Pipeline) publish(ctx context.Context, job ledger.Job, res naming.Result, working *os.File, attempt int, log *slog.Logger) Outcome {
	root := p.cfg.Storage.Consume

	// One token for the whole ATTEMPT, not one per candidate name.
	//
	// The claim is what makes publication exclusive, and it is guarded by this
	// token. Minting a fresh one for each candidate made an attempt that
	// advanced past an occupied name compete with ITSELF: it had claimed under
	// the first candidate's token, and the claim for the second candidate
	// arrived with a different one, so the ledger correctly reported that a
	// live attempt already held the job. The attempt stood down in favour of
	// itself, the delivery went back, and the next one recovered a job whose
	// reserved name was occupied by the foreign file -- so a collision with a
	// file the system did not publish ended as a hold instead of the suffixed
	// delivery the contract requires.
	token := nonce()

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

		out, done := p.linkIntoPlace(ctx, job, root, candidate, key, working, attempt, n, token, log)
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
//
// `token` identifies the ATTEMPT and is the same across every candidate this
// attempt tries; see publish for why it cannot be per-candidate.
func (p *Pipeline) linkIntoPlace(ctx context.Context, job ledger.Job, root, candidate, key string, working *os.File, attempt, seq int, token string, log *slog.Logger) (Outcome, bool) {
	final := filepath.Join(root, candidate)
	// Attempt-private: two attempts on one job must not stage over each other.
	tmp := filepath.Join(root, tempPrefix+job.JobID+"."+nonce()+".tmp")

	// One verified directory descriptor for every operation below. Creating,
	// linking and removing by pathname re-resolves the root each time, so a
	// root replaced after its verification redirects exactly the writes the
	// verification existed to protect. Nothing here re-resolves it.
	dir, derr := storage.OpenDir(root)
	if derr != nil {
		cat := jobs.Category(storage.RejectionCategory(derr))
		log.Error("the destination root could not be opened for publication",
			slog.String("event", "destination_root_unusable"),
			slog.String("category", string(cat)))
		out, _ := p.holdOr(ctx, job, cat, attempt, derr)
		return out, true
	}
	defer func() { _ = dir.Close() }()

	if err := p.stageBesideDestination(dir, working, filepath.Base(tmp)); err != nil {
		cat := jobs.Category(storage.RejectionCategory(err))
		log.Error("could not stage the document beside its destination",
			slog.String("event", "staging_failed"), slog.String("category", string(cat)))
		out, _ := p.holdOr(ctx, job, cat, attempt, err)
		return out, true
	}
	// The identity of the bytes about to be linked. After a successful link the
	// destination IS this inode, which is what makes ownership provable later.
	staged, serr := dir.Identify(filepath.Base(tmp))
	if serr != nil {
		out, _ := p.holdOr(ctx, job, jobs.Category(storage.RejectionCategory(serr)), attempt, serr)
		return out, true
	}
	linked := false
	// Set when this attempt does not know whether its receipt committed. The
	// staged file must then survive: if the receipt DID commit, it is the only
	// copy of an authorised publication, and recovery finds it by name and
	// finishes the reveal. Removing it would leave a receipt describing a
	// document that no longer exists.
	keepStaged := false
	defer func() {
		if keepStaged {
			log.Warn("leaving this attempt's staged document in place; its publication may be recorded",
				slog.String("event", "staged_document_retained"))
			return
		}
		if err := dir.RemoveIfOurs(filepath.Base(tmp), staged); err != nil {
			log.Warn("could not remove the staged link",
				slog.String("event", "staged_link_left_behind"),
				slog.String("error_kind", storage.RejectionCategory(err)))
			return
		}
		if linked {
			return
		}
		// Staged, never linked, and now gone. The inode is free for the
		// filesystem to hand to anybody, so this job must stop claiming it:
		// otherwise a stranger's file created later at this job's reserved
		// name can carry a recycled identity the job still recognises as its
		// own.
		fctx, fcancel := durably(ctx)
		defer fcancel()
		if ferr := p.led.ForgetStagedInode(fctx, job.JobID, int64(staged.Device), int64(staged.Inode)); ferr != nil {
			log.Warn("could not withdraw the claim on a temporary that was never linked",
				slog.String("event", "staged_identity_not_withdrawn"),
				slog.String("error_kind", logging.ErrorKind(ferr)))
		}
	}()

	// Verify the bytes that are about to become visible, through a descriptor
	// checked against that identity, immediately before the link.
	//
	// Everything earlier in this path verified a PATHNAME: the source was
	// verified and then copied by name, the working copy was verified and then
	// linked by name, and each of those names could have been pointed at
	// something else afterwards. The previous design noticed a mismatch by
	// hashing the destination AFTER publishing it, which is too late -- the
	// consumer can already have taken it. Reading it here costs one pass over
	// the file and makes "only complete, verified content becomes visible" a
	// fact about the bytes rather than about the last name that referred to
	// them.
	// The descriptor is held open across the link below. Hashing it and then
	// linking its PATHNAME are two operations on two different things: anything
	// that replaces the temporary in between is what gets published, and the
	// receipt then describes bytes that were never at the destination. The link
	// is issued from this descriptor, so the file that appears at the
	// destination is the file these bytes were read from.
	stagedFile, stagedSum, stagedSize, verr := openAndFingerprintOwnTemp(dir, filepath.Base(tmp), staged)
	if stagedFile != nil {
		defer func() { _ = stagedFile.Close() }()
	}
	if verr != nil {
		cat := jobs.Category(storage.RejectionCategory(verr))
		log.Error("could not verify the staged document before publishing it",
			slog.String("event", "staged_verification_failed"),
			slog.String("category", string(cat)))
		out, _ := p.holdOr(ctx, job, cat, attempt, verr)
		return out, true
	}
	if job.Fingerprint != nil && !bytes.Equal(stagedSum, job.Fingerprint) {
		log.Error("the staged document is not this job's content; not publishing it",
			slog.String("event", "staged_content_mismatch"),
			slog.String("category", string(jobs.CategorySourceMutated)))
		return p.hold(ctx, job, jobs.CategorySourceMutated, attempt), true
	}

	// Claim the right to publish. Exclusive per ATTEMPT, not per process: the
	// token is what the ledger guards on, because every handler in one renamer
	// shares the instance name and two of them holding "the exclusive claim"
	// at once is not exclusivity. Re-claiming under the same token updates the
	// reserved name and the staged identity, which is what an advance past an
	// occupied destination needs.
	claim, cerr := p.led.ClaimPublication(ctx, job.JobID, candidate,
		int64(staged.Inode), int64(staged.Device), attempt, token, p.cfg.PublishTakeoverAfter)
	switch {
	case errors.Is(cerr, ledger.ErrPublicationInProgress):
		log.Info("another attempt is publishing this job; standing down",
			slog.String("event", "publication_in_progress"),
			slog.String("held_by", ledger.ClaimHolder(cerr)))
		return deferred(ledger.ClaimHolder(cerr), cerr), true
	case errors.Is(cerr, ledger.ErrOutcomeAlreadyRecorded):
		// Read what stands. Reporting `delivered` here was a guess, and a
		// wrong one whenever the preserved outcome was a hold.
		if out, ok := p.settleAgainstPreserved(ctx, job.JobID, log); ok {
			return out, true
		}
		return unsettled(cerr), true
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

	// Still the publisher of record? Asked BEFORE the link, not after it.
	//
	// A takeover is a timeout, and a timeout cannot tell a dead worker from a
	// slow one: the slow one wakes up holding a staged file, full filesystem
	// access, and a claim that now belongs to somebody else. Checking after the
	// link meant such an attempt first CREATED a second copy at a name a
	// consumer watches -- a real second document, briefly consumable, and on a
	// directory Paperless polls every second "briefly" is enough -- and only
	// then discovered it should not have. The question is asked here, where the
	// answer can still prevent the copy instead of apologising for it.
	sctx, scancel := durably(ctx)
	pre, perr := p.led.Standing(sctx, job.JobID, token)
	scancel()
	switch {
	case perr != nil:
		// Unknown standing is not permission to publish. Returning the delivery
		// costs a redelivery; linking on a guess can cost a second document.
		log.Warn("could not confirm this attempt still holds the publication before linking",
			slog.String("event", "standing_unreadable"),
			slog.String("error_kind", logging.ErrorKind(perr)))
		return unsettled(perr), true
	case pre.HasReceipt:
		log.Info("a delivery for this job already stands; not linking a second copy",
			slog.String("event", "publication_superseded"))
		if out, ok := p.settleAgainstPreserved(ctx, job.JobID, log); ok {
			return out, true
		}
		return settled("delivered", jobs.StateDelivered, ""), true
	case !pre.HeldBySelf:
		log.Info("this attempt no longer holds the publication claim; standing down before linking",
			slog.String("event", "publication_in_progress"),
			slog.String("held_by", pre.Holder))
		return deferred(pre.Holder, ledger.ErrPublicationInProgress), true
	case jobs.IsTerminal(jobs.State(pre.State)):
		// Somebody has already decided this job's outcome while this attempt
		// was between its claim and its link. That is the ordinary shape of a
		// takeover: the claim expires, a sibling recovers the job, finds no
		// destination, and records `uncertain` because a filesystem handoff
		// cannot tell a publication that never happened from one whose file was
		// taken. Linking now would put a consumable document into the
		// consumer's directory for a job whose durable record says it is closed
		// -- a file no receipt describes and nothing will reconcile.
		//
		// The attempt stands down and settles against what stands. Its source
		// is untouched, so an operator resolving the uncertain job has
		// everything they need.
		log.Warn("this job reached a terminal outcome while this attempt was publishing; not linking",
			slog.String("event", "publication_superseded"),
			slog.String("state", pre.State))
		if out, ok := p.settleAgainstPreserved(ctx, job.JobID, log); ok {
			return out, true
		}
		return unsettled(ledger.ErrOutcomeAlreadyRecorded), true
	}

	// ---------------------------------------------------------------------
	// Publication, in the only order that survives its own failures.
	//
	// The staged temporary is already a dotfile in the destination directory,
	// hard-linked to the bytes verified a moment ago. The consumer ignores
	// dotfiles, so the document is published-but-invisible: everything that
	// can go wrong from here leaves it hidden rather than exposed.
	//
	// 1. Commit the receipt, under the job row's lock, refusing if anybody has
	//    closed the job. Nothing is visible, so a refusal withdraws nothing.
	// 2. Reveal by atomic rename, which never replaces.
	// 3. Mark the job delivered.
	//
	// The old order was link-then-record, and its failure was the one that
	// cannot be undone: an attempt whose claim had expired put a consumable
	// document into the consumer's directory for a job a sibling had already
	// closed, and could only apologise afterwards.
	//
	// The filesystem and the ledger still do not share a transaction. What
	// this buys is that every gap between them is survivable: a document that
	// is hidden can be revealed or removed later from what the ledger says,
	// and a document that is visible always has a committed receipt behind it.
	// ---------------------------------------------------------------------
	receipt := ledger.Receipt{
		JobID: job.JobID, DestinationRoot: root, DeliveredName: candidate,
		SizeBytes: stagedSize, Fingerprint: stagedSum, Attempt: attempt,
		// The file this receipt is about, identified before it was visible.
		PublishedDevice: int64(staged.Device), PublishedInode: int64(staged.Inode),
	}
	// Refuse early if the destination is already taken, so the common case
	// does not commit a receipt it then cannot honour. The reveal re-checks
	// atomically; this only avoids the wasted commit.
	if _, ierr := storage.Identify(final); ierr == nil {
		return p.resolveOccupiedDestination(ctx, job, root, candidate, key, final, attempt, log)
	}

	// The interval the job can be closed under this attempt. Held open, if
	// armed, so a consumer has a real chance to see anything this attempt has
	// put in the destination directory -- the point being that there is
	// nothing to see.
	p.faults.Pause(config.FaultHoldBeforeCommit, log)

	cctx, ccancel := durably(ctx)
	cerr2 := p.led.CommitPublication(cctx, receipt)
	ccancel()
	switch {
	case errors.Is(cerr2, ledger.ErrOutcomeAlreadyRecorded):
		// The job was closed while this attempt was working. Nothing was ever
		// visible: the document is still the dotfile the deferred cleanup
		// above removes. This is the case that used to expose a document.
		log.Warn("this job was closed while this attempt was publishing; nothing was made visible",
			slog.String("event", "publication_superseded"))
		if out, ok := p.settleAgainstPreserved(ctx, job.JobID, log); ok {
			return out, true
		}
		return unsettled(cerr2), true
	case errors.Is(cerr2, ledger.ErrPublicationSuperseded):
		log.Info("a delivery for this job already stands; not making a second document visible",
			slog.String("event", "publication_superseded"))
		if out, ok := p.settleAgainstPreserved(ctx, job.JobID, log); ok {
			return out, true
		}
		return settled("delivered", jobs.StateDelivered, ""), true
	case cerr2 != nil:
		// An error here does not say whether the transaction committed. A
		// connection lost at COMMIT is indistinguishable from one lost before
		// it, and no amount of ordering fixes that -- so this attempt must
		// leave the system in a state that survives either answer.
		//
		// It does not reveal, so nothing becomes visible for a publication
		// that may not be recorded. And it does not remove the staged file,
		// because if the receipt did commit that file is the only copy of an
		// authorised publication; recovery finds it by name and finishes it.
		// The cost is a dotfile that may be redundant, which the next attempt
		// resolves. The alternative is a receipt describing a document nobody
		// has.
		keepStaged = true
		log.Error("could not confirm the publication was committed; the document stays invisible and is kept",
			slog.String("event", "publication_not_confirmed"),
			slog.String("dependency", "postgres_primary"))
		return unsettled(cerr2), true
	}

	// Committed. From here the document is authorised, and the fault points
	// below hold the window a consumer could act in -- which is now the window
	// AFTER the receipt exists, not before it.
	p.faults.Fire(config.FaultAfterLink, log)
	p.faults.Pause(config.FaultHoldAfterLink, log)

	err := dir.RevealNoReplace(filepath.Base(tmp), candidate)
	published := err == nil
	if published {
		linked = true
	}

	switch {
	case published:
		size, fp := stagedSize, stagedSum
		_ = fp
		absent := false
		switch _, ierr := storage.Identify(final); {
		case ierr == nil:
		case errors.Is(ierr, fs.ErrNotExist):
			// Taken by the consumer between the reveal and this read. The
			// publication happened and the receipt already records it.
			absent = true
			log.Info("the destination was removed immediately after publication; the delivery stands",
				slog.String("event", "destination_consumed_immediately"))
		case errors.Is(ierr, storage.ErrMutated):
			absent = true
			log.Warn("the destination was replaced immediately after publication; the delivery stands",
				slog.String("event", "destination_replaced_after_publication"))
		default:
			log.Warn("the published destination could not be examined afterwards",
				slog.String("event", "destination_unexaminable"),
				slog.String("error_kind", storage.RejectionCategory(ierr)))
		}

		dctx, dcancel := durably(ctx)
		defer dcancel()
		if derr := p.led.MarkDelivered(dctx, job.JobID, attempt); derr != nil {
			// The document IS visible and its receipt IS committed. Only the
			// final state write failed, which recovery completes from the
			// receipt. Reporting this as a failed publication would be false.
			log.Error("published and recorded, but the job could not be closed",
				slog.String("event", "delivery_state_not_closed"),
				slog.String("dependency", "postgres_primary"))
			return unsettled(derr), true
		}
		if absent {
			if nerr := p.led.NoteDeliveredFileAbsent(dctx, job.JobID); nerr != nil {
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

	case errors.Is(err, fs.ErrNotExist):
		// This attempt's staged file is gone, so it is not the one that will
		// reveal it. Under the current order that means the publication was
		// already committed -- it must have been, or there would be nothing to
		// reveal -- and another attempt's recovery finished it. Settling
		// against what stands is right; treating it as a failed publication
		// would report a document that IS delivered as lost, and withdrawing
		// anything here would remove somebody's completed delivery.
		log.Info("another attempt completed this publication while this one held its staged file",
			slog.String("event", "publication_completed_elsewhere"))
		if out, ok := p.settleAgainstPreserved(ctx, job.JobID, log); ok {
			return out, true
		}
		return settled("delivered", jobs.StateDelivered, ""), true

	case errors.Is(err, storage.ErrDestinationExists):
		return p.resolveOccupiedDestination(ctx, job, root, candidate, key, final, attempt, log)

	default:
		cat := jobs.Category(storage.RejectionCategory(err))
		log.Error("publication failed",
			slog.String("event", "publication_failed"),
			slog.String("category", string(cat)))

		// Reached only when the link itself failed, which `published` now
		// decides rather than the errno. link(2) is atomic: a refusal means no
		// directory entry was created, so nothing was published and the claim
		// should not stand. Leaving it standing left the job in `publishing`,
		// which is the recovery path's input -- so the next delivery did not
		// retry the write, it "recovered" it, found no destination, and
		// recorded `uncertain`. A destination the kernel had plainly refused
		// became an unresolvable outcome instead of a retry that would have
		// recorded the real reason.
		//
		// Only definite refusals qualify even here. An ambiguous failure must
		// keep the claim, because then a publication may in fact have happened.
		if definitelyNotPublished(err) {
			actx, acancel := durably(ctx)
			aerr := p.led.AbandonPublication(actx, job.JobID, token, attempt, string(cat))
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

// settleAgainstPreserved settles against the outcome that actually stands.
//
// The ledger refuses a write that would replace a durable outcome, and returns
// ErrOutcomeAlreadyRecorded. The callers used to answer that refusal by
// asserting an outcome of their own -- the claim path reported `delivered`, the
// hold path reported `held` -- without reading what was preserved. A job that
// is `held` was therefore reported and counted as a delivery, and a job that is
// `delivered` as a hold. The refusal says only "somebody got there first"; what
// they wrote has to be read.
//
// `ok` is false when the preserved outcome could not be read, in which case the
// delivery goes back rather than being labelled from a guess.
func (p *Pipeline) settleAgainstPreserved(ctx context.Context, jobID string, log *slog.Logger) (Outcome, bool) {
	fresh, err := p.led.ReadJob(ctx, jobID)
	if err != nil {
		log.Warn("a durable outcome is recorded but could not be read; returning the delivery",
			slog.String("event", "preserved_outcome_unreadable"),
			slog.String("error_kind", logging.ErrorKind(err)))
		return Outcome{}, false
	}
	cat := jobs.Category(categoryOfJob(fresh))
	log.Info("a durable outcome is already recorded for this job; settling against it",
		slog.String("event", "outcome_preserved"),
		slog.String("state", string(fresh.State)),
		slog.String("category", string(cat)))
	return settled(string(fresh.State), fresh.State, cat), true
}

// categoryOfJob reports a job's failure category, or "" when it has none.
func categoryOfJob(j ledger.Job) string {
	if j.FailureCategory == nil {
		return ""
	}
	return *j.FailureCategory
}

// recordAbsentDelivery records a delivery whose file has already gone.
//
// Reconciliation identifies the destination and then reads it, and a consumer
// can take the document between those two operations -- Paperless polls every
// second. The old code ran that ENOENT through the generic storage classifier
// and held the job as `source_absent`: a missing SOURCE reported for a
// publication that had happened, with the source sitting untouched in the
// incoming directory. The delivery is recorded from what the job already knows
// about its own content, and the absence is recorded with it.
func (p *Pipeline) recordAbsentDelivery(ctx context.Context, job ledger.Job, root, name string, attempt int, log *slog.Logger) Outcome {
	if job.Fingerprint == nil {
		// Nothing to record a receipt from. Uncertain is the honest answer
		// rather than a receipt with invented content.
		log.Error("the document went before it could be read and this job has no recorded fingerprint",
			slog.String("event", "publication_uncertain"),
			slog.String("category", string(jobs.CategoryPublicationUncertain)))
		return p.uncertain(ctx, job, attempt)
	}
	size := int64(0)
	if job.SizeBytes != nil {
		size = *job.SizeBytes
	}
	rctx, rcancel := durably(ctx)
	defer rcancel()
	receipt := ledger.Receipt{
		JobID: job.JobID, DestinationRoot: root, DeliveredName: name,
		SizeBytes: size, Fingerprint: job.Fingerprint, Attempt: attempt,
	}
	if err := p.led.RecordDelivered(rctx, receipt, true); err != nil {
		return unsettled(err)
	}
	if nerr := p.led.NoteDeliveredFileAbsent(rctx, job.JobID); nerr != nil {
		log.Warn("could not record that the delivered file was already absent",
			slog.String("event", "absence_note_failed"))
	}
	log.Info("recovered a publication whose file had already been taken",
		slog.String("event", "delivery_reconciled"),
		slog.Bool("destination_already_absent", true))
	return settled("reconciled", jobs.StateDelivered, "")
}

// withdrawDuplicate removes a second copy this attempt has just created, when
// the durable record says a DIFFERENT file is the delivery that stands.
//
// This is the one place a published file is removed, and the condition it turns
// on used to be wrong in a way that destroyed documents. It removed this
// attempt's file whenever ANY receipt existed for the job -- but the ordinary
// way a receipt appears while this attempt is paused is that a sibling
// RECONCILED this very file: it found the destination, recognised the inode as
// the job's own, hashed it and recorded the delivery. The receipt then
// described the file this attempt had linked, and the attempt deleted it. One
// document, one receipt saying it was delivered, and nothing at the
// destination.
//
// So the question is not "does a receipt exist" but "does the receipt describe
// a different file from the one I just linked". The receipt now carries the
// device and inode of the file it was written for, which answers it exactly.
// When it does not -- a reconciled delivery of a destination the consumer had
// already taken cannot identify a file that is gone -- the name is compared
// instead, and an unresolvable case is left alone: leaving a duplicate is
// recoverable, deleting the only copy is not.
func (p *Pipeline) withdrawDuplicate(ctx context.Context, job ledger.Job, dir *storage.Dir, candidate string, staged storage.Entry, token string, attempt int, log *slog.Logger) (Outcome, bool) {
	sctx, scancel := durably(ctx)
	defer scancel()
	standing, serr := p.led.Standing(sctx, job.JobID, token)
	if serr != nil {
		// Unknown standing is not evidence of a duplicate. Leave the document
		// in place and let the ordinary path record it; a receipt that cannot
		// be written is handled there.
		log.Warn("could not read the publication standing after linking",
			slog.String("event", "standing_unreadable"),
			slog.String("error_kind", logging.ErrorKind(serr)))
		return Outcome{}, false
	}
	if !standing.HasReceipt {
		// No competing delivery is recorded. Losing the claim without a
		// receipt is not a reason to remove anything: this attempt's link may
		// well be the publication that gets recorded.
		return Outcome{}, false
	}

	switch {
	case standing.ReceiptInode != 0 &&
		standing.ReceiptDevice == int64(staged.Device) &&
		standing.ReceiptInode == int64(staged.Inode):
		// The receipt describes the file this attempt linked. A sibling
		// reconciled it. There is no duplicate; there is one document and a
		// receipt for it.
		log.Info("the recorded delivery describes this attempt's own file; nothing to withdraw",
			slog.String("event", "publication_reconciled_by_sibling"))
		return settled("delivered", jobs.StateDelivered, ""), true
	case standing.ReceiptInode == 0 && standing.DeliveredName == candidate:
		// A receipt for this very name that cannot identify a file: a delivery
		// reconciled from a destination that had already gone. Removing what is
		// at that name now would be removing a file nothing has identified as a
		// duplicate.
		log.Warn("a delivery stands for this name but does not identify a file; leaving the destination alone",
			slog.String("event", "duplicate_undecidable"))
		return settled("delivered", jobs.StateDelivered, ""), true
	}

	// A different file is the delivery of record, so what this attempt linked
	// is a second copy. It is removed through the same directory descriptor
	// that created it and only while it is still this attempt's own inode --
	// identifying by pathname and then removing by pathname is how a
	// replacement gets deleted instead.
	removed, rerr := dir.RemoveOwned(candidate, staged)
	if rerr != nil {
		log.Error("could not remove the duplicate this attempt published",
			slog.String("event", "duplicate_not_withdrawn"),
			slog.String("error_kind", storage.RejectionCategory(rerr)))
	}
	log.Warn("another attempt's delivery already stands; withdrawing this attempt's copy",
		slog.String("event", "duplicate_publication_withdrawn"),
		slog.Bool("removed", removed))
	return settled("delivered", jobs.StateDelivered, ""), true
}

// fingerprintPublished reads back a document this attempt published, through a
// descriptor it has proved is the file it linked.
//
// `expect` is the identity of the staged temporary, which after a successful
// link is also the identity of the destination. Opening the pathname again and
// hashing whatever answers is the check/use gap: a consumer can take the
// document and something else can take the name in between, and the hash would
// then be compared against this job's fingerprint as though it meant something.
// A changed identity comes back as ErrMutated and is handled as what it is.
func fingerprintPublished(root, candidate string, expect storage.Entry) ([]byte, int64, error) {
	f, _, err := storage.OpenExpected(root, candidate, expect, false)
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()
	return storage.FingerprintFile(f)
}

// openAndFingerprintOwnTemp opens this attempt's staged temporary, checks it is
// still the file that was identified, hashes it, and RETURNS THE DESCRIPTOR
// still open.
//
// The descriptor is the point. Hashing bytes and then publishing a name is a
// claim about a name; publishing the descriptor those bytes were read from is a
// claim about the file. The caller closes it after the link.
func openAndFingerprintOwnTemp(dir *storage.Dir, name string, expect storage.Entry) (*os.File, []byte, int64, error) {
	f, _, err := dir.OpenOwn(name, expect)
	if err != nil {
		return nil, nil, 0, err
	}
	sum, size, err := storage.FingerprintFile(f)
	if err != nil {
		_ = f.Close()
		return nil, nil, 0, err
	}
	return f, sum, size, nil
}

// fingerprintOwnTemp is fingerprintPublished for a temporary this attempt
// staged. Those are dotfiles by design -- the destination directory is watched
// by a consumer, and a leading dot is how a file says "not a submission" --
// which the ordinary opener refuses, rightly, because a dotfile appearing in a
// watched directory is somebody else's business. This one is ours: the name
// carries a nonce this attempt generated, and the identity is still checked.
func fingerprintOwnTemp(root, name string, expect storage.Entry) ([]byte, int64, error) {
	f, _, err := storage.OpenOwnTemp(root, name, expect)
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()
	return storage.FingerprintFile(f)
}

// definitelyNotPublished reports whether a link failure proves no directory
// entry was created.
//
// It is consulted only when the link itself failed. Whether the link succeeded
// is now reported separately by PublishExclusive, because an errno cannot
// answer it: EACCES from the post-link unlink and EACCES from the link are the
// same value and opposite facts.
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

	// What makes an occupant this job's document is the RECEIPT, not the inode.
	//
	// A retained device/inode pair stops meaning anything the moment the file
	// holding it is gone: the filesystem is free to hand that identity to the
	// next file created, and a foreign submission landing at this job's
	// reserved name can carry it. Equal bytes do not rescue that -- two
	// distinct submissions are allowed to be byte-identical, and the contract
	// says they stay distinct.
	//
	// Publication now commits the receipt while the document is still an
	// invisible dotfile and only then reveals it, so a file visible at a job's
	// reserved name always has a committed receipt behind it. That makes the
	// receipt both necessary and sufficient here, and makes the inode a
	// corroborating detail rather than the authority.
	existing, rerr := p.led.GetReceipt(ctx, job.JobID)
	switch {
	case rerr == nil:
	case errors.Is(rerr, ledger.ErrNotFound):
		existing = ledger.Receipt{}
	default:
		return unsettled(rerr), true
	}
	ours := existing.JobID != "" &&
		existing.DestinationRoot == root && existing.DeliveredName == candidate

	if ours {
		sum, size, serr := fingerprintPublished(root, candidate, got)
		if errors.Is(serr, fs.ErrNotExist) || errors.Is(serr, storage.ErrMutated) {
			// Same reasoning as the recovery path: a file that is gone cannot
			// be shown to have been this job's, because its identity is free
			// for the filesystem to hand to anybody.
			log.Error("the occupied destination went before it could be read; not adopting it",
				slog.String("event", "publication_uncertain"),
				slog.String("category", string(jobs.CategoryPublicationUncertain)))
			return p.uncertain(ctx, job, attempt), true
		}
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

	// No receipt naming this destination for this job, so the occupant is not
	// this job's document however much it resembles one. Whether the bytes
	// match is recorded but decides nothing: equal content is exactly the case
	// the identity check exists for, and treating it as ownership would be
	// implicit deduplication.
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
	// Any live claim, whoever holds it.
	//
	// This used to exempt a claim whose holder name matched this process, on
	// the reasoning that a claim of "our own" could not be a live sibling. Every
	// handler in one renamer shares that name: with concurrency 2 the second
	// delivery of a job recovered straight past a claim the FIRST handler was
	// still publishing under, which is the exact race the claim exists to
	// prevent, reached by the one path that skipped the check. Recovery holds no
	// claim of its own when it gets here -- Process routes a `publishing` job
	// here before anything is claimed -- so a fresh claim always belongs to some
	// other attempt, in this process or another.
	if holder, fresh, herr := p.led.PublicationClaimState(ctx, job.JobID, p.cfg.PublishTakeoverAfter); herr == nil {
		if fresh && holder != "" {
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
		// The RECEIPT decides, not the inode.
		//
		// A retained device/inode pair is authority only while the file
		// holding it exists; once it is gone the filesystem may hand that
		// identity to the next file created, and a foreign submission landing
		// at this job's reserved name then carries it. Equal bytes do not
		// help: byte-identical submissions are required to stay distinct.
		//
		// Publication commits the receipt while the document is still an
		// invisible dotfile and reveals it only afterwards, so a file visible
		// at a job's reserved name always has a committed receipt behind it.
		// No receipt naming this destination means the occupant is somebody
		// else's, whatever its inode or its contents.
		existing, rerr := p.led.GetReceipt(ctx, job.JobID)
		switch {
		case rerr == nil:
		case errors.Is(rerr, ledger.ErrNotFound):
			existing = ledger.Receipt{}
		default:
			return unsettled(rerr)
		}
		if existing.JobID == "" || existing.DestinationRoot != root || existing.DeliveredName != name {
			log.Error("the reserved destination holds a file no receipt of this job's describes",
				slog.String("event", "destination_conflict"),
				slog.String("category", string(jobs.CategoryDestinationConflict)))
			return p.hold(ctx, job, jobs.CategoryDestinationConflict, attempt)
		}
		sum, size, serr := fingerprintPublished(root, name, got)
		if errors.Is(serr, fs.ErrNotExist) || errors.Is(serr, storage.ErrMutated) {
			// Identified as this job's, then gone before it could be read --
			// a consumer taking it is the ordinary reason. The publication
			// happened and the delivery stands; calling this a missing source
			// would lose a completed delivery from the accounting.
			//
			// Identity alone cannot carry this, and neither can identity plus
			// size. A device/inode pair is reused by the filesystem as soon as
			// the file that held it is gone, so "the inode this job once
			// linked" and "this job's document" stop being the same statement
			// the moment the file disappears -- and this branch is reached
			// precisely because it disappeared. A foreign file created later at
			// this job's reserved name can carry the recycled identity, and any
			// size agreement it happens to have proves nothing about it.
			//
			// So this attempt says what it actually knows: a publication may
			// have happened and cannot be confirmed. That is what `uncertain`
			// is for, and a person resolves it with the source, which is still
			// in the incoming root.
			log.Error("the destination went before it could be read; ownership cannot be established from a vanished file",
				slog.String("event", "publication_uncertain"),
				slog.String("category", string(jobs.CategoryPublicationUncertain)))
			return p.uncertain(ctx, job, attempt)
		}
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
			PublishedDevice: int64(got.Device), PublishedInode: int64(got.Inode),
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
		// Nothing at the reserved name. A committed receipt changes what that
		// means, because publication commits the receipt BEFORE it reveals:
		//
		//   receipt + staged dotfile  -> this attempt died between the two.
		//                                The document is present, authorised
		//                                and invisible; finish the reveal.
		//   receipt + nothing staged  -> it was revealed and then taken, which
		//                                is the ordinary consumer case. The
		//                                delivery stands and its file is gone.
		//   no receipt                -> unchanged: uncertain, below.
		if existing, rerr := p.led.GetReceipt(ctx, job.JobID); rerr == nil &&
			existing.DestinationRoot == root && existing.DeliveredName == name {
			if dir, derr := storage.OpenDir(root); derr == nil {
				defer func() { _ = dir.Close() }()
				if staged, ferr := dir.FindStagedFor(job.JobID); ferr == nil && staged != "" {
					if rverr := dir.RevealNoReplace(staged, name); rverr == nil {
						if merr := p.led.MarkDelivered(ctx, job.JobID, attempt); merr != nil {
							return unsettled(merr)
						}
						log.Info("finished a publication that was committed but never revealed",
							slog.String("event", "publication_reveal_completed"))
						return settled("reconciled", jobs.StateDelivered, "")
					}
				}
			}
			// Recorded, and its file is not here. Keep the delivery.
			if merr := p.led.MarkDelivered(ctx, job.JobID, attempt); merr != nil {
				return unsettled(merr)
			}
			if nerr := p.led.NoteDeliveredFileAbsent(ctx, job.JobID); nerr != nil {
				log.Warn("could not record that the delivered file was already absent",
					slog.String("event", "absence_note_failed"))
			}
			log.Info("a committed delivery whose file is no longer present; the delivery stands",
				slog.String("event", "delivery_reconciled"))
			return settled("reconciled", jobs.StateDelivered, "")
		}
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
// stageBesideDestination places this attempt's bytes inside the destination
// directory, through the directory's own descriptor.
//
// It used to create the temporary by PATHNAME, which left the one write in the
// publication path that the root verification did not cover: a root repointed
// between its verification and this call put the staged file somewhere else
// entirely. Creating it relative to the verified descriptor means the file is
// inside the accepted directory or it is nowhere.
//
// The working copy is opened once and both routes use that descriptor: a link
// when the two roots share a filesystem, a copy when they do not.
func (p *Pipeline) stageBesideDestination(dir *storage.Dir, wf *os.File, tmpName string) error {
	if lerr := dir.LinkFromDescriptor(wf, tmpName); lerr == nil {
		return nil
	} else if errors.Is(lerr, storage.ErrDestinationExists) {
		return lerr
	}
	// Different filesystems: copy into a file created inside the destination
	// directory, then flush it before anybody is told it exists.
	return dir.CopyInto(wf, tmpName)
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
		// attempt settles against WHAT STANDS -- previously it reported `held`
		// with its own category, so a delivery that had beaten it was recorded
		// in the metrics and the log as a hold.
		log := p.log.With(slog.String("job_id", job.JobID))
		if out, ok := p.settleAgainstPreserved(ctx, job.JobID, log); ok {
			return out
		}
		return unsettled(err)
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
		// A refusal is not an outage. RecordUncertain is guarded so it cannot
		// demote a delivery that already stands, and that guard firing was
		// being returned as a bare error -- which the processor reported as a
		// PostgreSQL failure and answered by detaching the consumer. The
		// database did exactly what it was asked to do.
		if errors.Is(err, ledger.ErrOutcomeAlreadyRecorded) {
			log := p.log.With(slog.String("job_id", job.JobID))
			if out, ok := p.settleAgainstPreserved(ctx, job.JobID, log); ok {
				return out
			}
		}
		return unsettled(err)
	}
	return settled("uncertain", jobs.StateUncertain, jobs.CategoryPublicationUncertain)
}
