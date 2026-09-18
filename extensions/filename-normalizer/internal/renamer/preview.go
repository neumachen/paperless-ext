package renamer

import (
	"context"
	"log/slog"
	"time"

	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/app"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/config"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/jobs"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/ledger"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/naming"
)

// Previewer is the renamer's dry-run mode.
//
// # Why it does not consume
//
// A dry run must leave sources, the operational ledger and queued work
// unchanged, and previewed work must still be processable afterwards. An
// earlier version got this wrong in a way that is worth recording, because the
// mistake is easy to repeat: it took deliveries off the real work queue,
// recorded delivery ownership, wrote a normalized name to the job row, and
// then acknowledged the message. Every one of those is a mutation, and the
// acknowledgement was the worst of them -- it consumed work that would then
// never be processed for real.
//
// A mode whose purpose is to change nothing therefore must not attach a
// consumer at all. This worker reads the ledger, computes what each waiting
// job's name WOULD be, and reports aggregate counts. It performs no write of
// any kind: no BeginDelivery, no RecordNormalized, no acknowledgement, no
// filesystem access beyond reading nothing at all.
//
// Detailed per-document output is deliberately not produced here. Routine
// output stays aggregate; a caller who needs the actual names asks the gRPC
// PreviewName RPC, which is the explicit, restricted surface for that.
type Previewer struct {
	base *app.Base
	cfg  config.RenamerConfig
	log  *slog.Logger
}

// NewPreviewer builds the dry-run worker.
func NewPreviewer(base *app.Base, cfg config.RenamerConfig) *Previewer {
	return &Previewer{base: base, cfg: cfg, log: base.Log.With(slog.String("component", "preview"))}
}

// previewInterval is how often the dry run reports.
const previewInterval = 10 * time.Second

// previewBatch bounds one pass.
const previewBatch = 500

// Run reports what would happen, until the context is cancelled.
func (p *Previewer) Run(ctx context.Context) {
	p.log.Warn("dry run: this instance does not consume the work queue",
		slog.String("event", "dry_run_enabled"),
		slog.String("policy_version", p.cfg.Policy.Identity))

	ticker := time.NewTicker(previewInterval)
	defer ticker.Stop()

	p.report(ctx)
	for {
		select {
		case <-ctx.Done():
			p.log.Info("dry run stopped", slog.String("event", "dry_run_stopped"))
			return
		case <-ticker.C:
			p.report(ctx)
		}
	}
}

// report computes outcomes for waiting jobs without writing anything.
func (p *Previewer) report(ctx context.Context) {
	var wouldPublish, examined int
	byCategory := map[string]int{}

	for _, state := range []jobs.State{jobs.StatePendingDispatch, jobs.StateDispatched, jobs.StateProcessing} {
		list, err := p.base.Ledger.JobsInState(ctx, state, previewBatch)
		if err != nil {
			p.log.Error("dry run could not read the ledger",
				slog.String("event", "dry_run_failed"),
				slog.String("dependency", "postgres_primary"))
			return
		}
		for _, job := range list {
			examined++
			switch {
			case job.PolicyVersion != p.cfg.Policy.Identity:
				byCategory[string(jobs.CategoryPolicyMismatch)]++
			// Work accepted before the destination was recorded is held by
			// real processing and must be counted as held here too. Counting
			// it as publishable told an operator that ambiguous historical
			// work would go out under the current configuration, which is the
			// opposite of what the pipeline does with it.
			case job.DestinationRootUnknown:
				byCategory[string(jobs.CategoryDestinationMismatch)]++
			case job.DestinationRoot != nil && *job.DestinationRoot != "" && *job.DestinationRoot != p.cfg.Storage.Consume:
				byCategory[string(jobs.CategoryDestinationMismatch)]++
			default:
				switch {
				default:
					// The name publication would actually reserve, not the
					// name the policy produces in isolation.
					//
					// Checking candidate 0 alone overstated what a dry run had
					// established: a job whose base name is already reserved by
					// a DIFFERENT job is published under a suffix, and the
					// suffixed name is a different string with its own
					// validity. Counting the base name as publishable therefore
					// answered a question the pipeline never asks. The
					// candidates are walked here the way publication walks
					// them, using reservations only -- this is a ledger read,
					// and a dry run touches no filesystem.
					cat, ok := p.wouldPublishAs(ctx, job)
					if ok {
						wouldPublish++
					} else {
						byCategory[cat]++
					}
				}
			}
		}
	}

	// Aggregate only: counts and closed-set categories, never a name.
	//
	// `examined` is reported alongside the counts because the two claims are
	// different: how many waiting jobs this pass looked at, and how many of
	// them the running policy would publish under the names actually available.
	attrs := []any{
		slog.String("event", "dry_run_report"),
		slog.Int("count", wouldPublish),
		slog.Int("examined", examined),
	}
	for cat, n := range byCategory {
		if jobs.SafeIdentifier(cat) {
			attrs = append(attrs, slog.Int("would_hold_"+cat, n))
		}
	}
	p.log.Info("dry run: what the running policy would do with the waiting jobs", attrs...)
}

// wouldPublishAs decides what the running policy would do with one waiting job,
// including which name it would actually get.
//
// Reservations decide the name; the filesystem decides nothing here and is not
// consulted. A name already reserved by another job is one this job cannot
// have, so the next candidate is tried, exactly as publication does -- and the
// candidate that is finally reached is the one whose validity matters.
func (p *Previewer) wouldPublishAs(ctx context.Context, job ledger.Job) (string, bool) {
	res, err := p.cfg.Policy.Normalize(job.SourceName, job.JobID)
	if err != nil {
		return naming.HoldCategory(err), false
	}
	root := p.cfg.Storage.Consume
	// Bounded, and the bound is part of the claim.
	//
	// Publication may walk to MaxCollisionSuffix, which is 9999: a dry run that
	// did the same would issue ten thousand reservation lookups for one job to
	// answer a question nobody asked. It walks a short way and says so when it
	// stops, which is the difference between a cheap report and a confident
	// wrong one.
	limit := p.cfg.Policy.MaxCollisionSuffix
	if limit > previewCollisionProbe {
		limit = previewCollisionProbe
	}
	for n := 0; n <= limit; n++ {
		candidate, cerr := p.cfg.Policy.Candidate(res, n)
		if cerr != nil {
			return string(jobs.CategoryNameTooLong), false
		}
		if verr := p.cfg.Policy.VerifyFinalName(candidate, job.JobID); verr != nil {
			return naming.HoldCategory(verr), false
		}
		owner, blocked, oerr := p.base.Ledger.ReservationOwner(ctx, root, naming.ReservationKey(candidate))
		switch {
		case oerr != nil:
			// The ledger could not say. Reporting this job as publishable
			// would be a guess in the direction that reads as reassurance.
			return string(jobs.CategoryLedgerUnavailable), false
		case owner == "" || (owner == job.JobID && !blocked):
			return "", true
		}
	}
	// Not decided within the probe. Reported as its own category rather than
	// counted publishable: "we did not look far enough" is not "it is fine".
	return previewCollisionUndecided, false
}

// previewCollisionProbe bounds how many collision candidates a dry run tries
// before reporting that it did not decide.
const previewCollisionProbe = 32

// previewCollisionUndecided is the category for a job whose destination name
// could not be settled within that probe.
const previewCollisionUndecided = "collision_undecided"
