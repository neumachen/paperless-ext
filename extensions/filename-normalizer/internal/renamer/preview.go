package renamer

import (
	"context"
	"log/slog"
	"time"

	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/app"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/config"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/jobs"
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
	var wouldPublish int
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
				res, err := p.cfg.Policy.Normalize(job.SourceName, job.JobID)
				switch {
				case err != nil:
					byCategory[naming.HoldCategory(err)]++
				default:
					// The name publication would actually reserve has to
					// satisfy the policy's invariants, and publication holds
					// the job when it does not. A preview that skipped that
					// check counted work as publishable that the pipeline
					// would refuse.
					if c, cerr := p.cfg.Policy.Candidate(res, 0); cerr != nil {
						byCategory[string(jobs.CategoryNameTooLong)]++
					} else if verr := p.cfg.Policy.VerifyFinalName(c, job.JobID); verr != nil {
						byCategory[naming.HoldCategory(verr)]++
					} else {
						wouldPublish++
					}
				}
			}
		}
	}

	// Aggregate only: counts and closed-set categories, never a name.
	attrs := []any{
		slog.String("event", "dry_run_report"),
		slog.Int("count", wouldPublish),
	}
	for cat, n := range byCategory {
		if jobs.SafeIdentifier(cat) {
			attrs = append(attrs, slog.Int("would_hold_"+cat, n))
		}
	}
	p.log.Info("dry run: what the running policy would do with the waiting jobs", attrs...)
}
