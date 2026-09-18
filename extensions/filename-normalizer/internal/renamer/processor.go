package renamer

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/app"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/broker"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/config"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/jobs"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/ledger"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/logging"
)

// Processor consumes the work queue with bounded concurrency.
type Processor struct {
	base     *app.Base
	cfg      config.RenamerConfig
	log      *slog.Logger
	consumer *broker.Consumer
	pipeline *Pipeline
}

// NewProcessor builds the consumer worker.
func NewProcessor(base *app.Base, cfg config.RenamerConfig) *Processor {
	log := base.Log.With(slog.String("component", "consumer"))
	p := &Processor{base: base, cfg: cfg, log: log}
	p.pipeline = NewPipeline(cfg, base.Ledger, log, base.Metrics)
	// Publish the configured bounds so an observer can check in-flight work
	// against this instance's own limit.
	base.Metrics.ConcurrencyLimit.Set(float64(cfg.Concurrency))
	base.Metrics.PrefetchLimit.Set(float64(cfg.Prefetch))
	p.consumer = broker.NewConsumer(base.Conn, broker.ConsumerOptions{
		Queue:       cfg.Broker.Queue,
		Prefetch:    cfg.Prefetch,
		Concurrency: cfg.Concurrency,
		ConsumerTag: "renamer/" + cfg.Instance,
		Logger:      log,
		// A delivery can only be settled safely once the outcome is durably
		// recorded, so there is no point holding deliveries while the durable
		// store cannot take that record. Refusing to attach leaves the
		// persistent messages in the queue instead.
		//
		// Reachability is not enough: a database whose migration never ran is
		// perfectly reachable, and a consumer attached to it would take
		// deliveries it could not settle. The gate therefore requires the same
		// usable schema that readiness requires.
		Gate: func(ctx context.Context) (bool, string) {
			if err := base.Ledger.PingPrimary(ctx); err != nil {
				return false, logging.ErrorKind(err)
			}
			if _, err := base.Ledger.SchemaReady(ctx); err != nil {
				return false, "schema_unusable"
			}
			return true, ""
		},
		DetachBackoff: cfg.Broker.ReconnectDelay,
		// A delivery already taken gets at most the shutdown budget to reach a
		// durable outcome, whether or not shutdown has begun.
		HandlerBudget: cfg.ShutdownTimeout,
		OnActive: func(active bool) {
			v := 0.0
			if active {
				v = 1
			}
			base.Metrics.ConsumerUp.Set(v)
		},
		OnDelivery: func(d broker.Delivery) {
			base.Metrics.DeliveryInFlight.Inc()
			if d.Redelivered {
				base.Metrics.Redeliveries.Inc()
			}
		},
		OnSettled: func(_ broker.Delivery, _ broker.Decision, took time.Duration) {
			base.Metrics.DeliveryInFlight.Dec()
			base.Metrics.DeliverySeconds.Observe(took.Seconds())
		},
	})
	return p
}

// InFlight reports deliveries currently being handled.
func (p *Processor) InFlight() int { return p.consumer.InFlight() }

// Run consumes until the context is cancelled.
func (p *Processor) Run(ctx context.Context) { p.consumer.Run(ctx, p.handle) }

// handle takes one delivery through the real path and returns its settlement.
//
// The contract this function keeps: acknowledge only after a safe outcome has
// been durably recorded. Every ack below is preceded by a committed ledger
// write, and every path that could not commit returns the delivery instead.
func (p *Processor) handle(ctx context.Context, d broker.Delivery) broker.Decision {
	msg, err := jobs.DecodeMessage(d.Body)
	if err != nil {
		// A payload this build must not interpret is dead-lettered rather than
		// requeued: retrying it would never succeed. The message stays visible
		// in the dead-letter queue, which is an artefact, not a completion.
		p.base.Metrics.Deliveries.WithLabelValues("rejected_contract").Inc()
		p.log.Error("delivery carries an unsupported contract",
			slog.String("event", "delivery_rejected"),
			slog.String("category", string(jobs.CategoryUnsupportedContract)),
			slog.Uint64("delivery_tag", d.DeliveryTag),
			slog.Bool("redelivered", d.Redelivered),
			slog.Int("contract_version", jobs.ContractVersion))
		return broker.Reject
	}

	log := p.log.With(
		slog.String("job_id", msg.JobID),
		slog.Int("attempt", msg.Attempt),
		slog.Bool("redelivered", d.Redelivered),
		slog.Int("attempts", d.DeliveryCount))

	log.Info("delivery received",
		slog.String("event", "delivery_received"),
		slog.Uint64("delivery_tag", d.DeliveryTag))

	job, err := p.base.Ledger.BeginDelivery(ctx, msg.JobID, msg.Attempt, d.Redelivered)
	switch {
	case errors.Is(err, ledger.ErrNotFound):
		// The broker references a job with no durable row. Requeueing cannot
		// create one, so the message is dead-lettered and the mismatch is made
		// visible instead of being retried forever.
		p.base.Metrics.Deliveries.WithLabelValues("rejected_unknown_job").Inc()
		log.Error("delivery references an unknown job",
			slog.String("event", "delivery_rejected"),
			slog.String("category", string(jobs.CategoryUnknownJob)))
		return broker.Reject
	case err != nil:
		// The durable store is unavailable. Nothing was recorded, so the
		// delivery must go back to the broker. What bounds the retry is the
		// ledger's delivery_attempts counter, not x-delivery-limit: an
		// explicit requeue does not advance a quorum queue's delivery count.
		p.base.Metrics.Deliveries.WithLabelValues("requeued").Inc()
		p.base.Metrics.LedgerErrors.WithLabelValues(logging.ErrorKind(err)).Inc()
		log.Error("could not record delivery ownership; returning the delivery and pausing consumption",
			slog.String("event", "delivery_requeued"),
			slog.String("dependency", "postgres_primary"),
			slog.String("category", string(jobs.CategoryLedgerUnavailable)),
			slog.String("error_kind", logging.ErrorKind(err)))
		p.consumer.RequestDetach()
		return broker.NackRequeue
	}

	switch job.State {
	case jobs.StateHeld, jobs.StateDelivered, jobs.StateUncertain:
		// A redelivery of a job that already reached a durable terminal
		// outcome. The outcome stands; repeating it would be noise, and
		// reprocessing it could publish a second copy.
		outcome := string(job.State)
		p.base.Metrics.Deliveries.WithLabelValues(terminalLabel(job.State)).Inc()
		log.Info("delivery settled against an existing terminal outcome",
			slog.String("event", "delivery_settled"),
			slog.String("state", outcome),
			slog.String("outcome", outcome),
			slog.String("category", categoryOf(job)))
		return broker.Ack
	}

	// Normalization, reservation and publication.
	out := p.pipeline.Process(ctx, job, msg.Attempt)

	// A deferral is not a fault. A sibling holds the publication claim, so
	// this attempt did nothing and puts the delivery back untouched.
	//
	// It is handled apart from the failure path below for three reasons, each
	// of which was wrong when the two shared a branch: it was logged as a
	// postgres_primary problem, which it is not; it detached the consumer, so
	// one contended job stalled every unrelated job this worker had; and it
	// spent a delivery against the job's durable retry budget, so two healthy
	// workers taking turns could hold a document that nothing was wrong with.
	if out.Deferred {
		if rerr := p.base.Ledger.ReleaseDelivery(ctx, msg.JobID, msg.Attempt, out.DeferredTo); rerr != nil {
			// The attempt stays counted. That is the safe direction: the
			// budget is a bound on retries, and over-counting ends in a hold
			// somebody can see rather than in an unbounded loop.
			log.Warn("could not return the deferred delivery attempt to the budget",
				slog.String("event", "delivery_attempt_not_released"),
				slog.String("error_kind", logging.ErrorKind(rerr)))
		}
		p.base.Metrics.Deliveries.WithLabelValues("deferred").Inc()
		log.Info("another attempt holds this job's publication; returning the delivery",
			slog.String("event", "delivery_deferred"),
			slog.String("held_by", out.DeferredTo))
		// A short bounded wait before the message goes back, so a contended
		// job does not spin between workers at broker speed. It holds one
		// handler slot, not the consumer, which is the difference from the
		// detach this replaces.
		select {
		case <-ctx.Done():
		case <-time.After(p.cfg.Broker.ReconnectDelay):
		}
		return broker.NackRequeue
	}

	if !out.Settled {
		// Nothing was committed, so the delivery must go back to the broker.
		p.base.Metrics.Deliveries.WithLabelValues("requeued").Inc()

		// Say which dependency actually failed. A refused destination and a
		// full disk used to be reported as a PostgreSQL problem, which sent an
		// operator to look at a database that was working perfectly.
		dep, cat := out.Dependency, out.Cause
		if dep == "" {
			dep, cat = "postgres_primary", jobs.CategoryLedgerUnavailable
		}
		// Count a ledger error only when the ledger is what failed. Every
		// storage retry used to land on this counter, so a disk with no space
		// read on the dashboard as a database outage -- and the alert built on
		// it would have pointed at the wrong dependency during an incident.
		if out.Err != nil && dep == "postgres_primary" {
			p.base.Metrics.LedgerErrors.WithLabelValues(logging.ErrorKind(out.Err)).Inc()
		}
		log.Error("could not reach a durable outcome; returning the delivery",
			slog.String("event", "delivery_requeued"),
			slog.String("dependency", dep),
			slog.String("category", string(cat)),
			slog.String("error_kind", logging.ErrorKind(out.Err)))

		// Detach only when the dependency itself is unusable. One job's
		// storage failure is that job's problem: pausing the whole consumer
		// for it stalls every unrelated job this worker holds, which is the
		// defect a deferral was already fixed for. A vanished or replaced root
		// is different -- nothing else will succeed either -- so that still
		// detaches, and readiness reports it.
		if dep == "postgres_primary" || cat == jobs.CategoryStorageUnavailable {
			p.consumer.RequestDetach()
		} else {
			// A bounded pause, so a persistently failing job does not spin
			// between redeliveries at broker speed while its budget runs down.
			select {
			case <-ctx.Done():
			case <-time.After(p.cfg.Broker.ReconnectDelay):
			}
		}
		return broker.NackRequeue
	}

	if out.Label != "dry_run" {
		p.base.Metrics.Deliveries.WithLabelValues(out.Label).Inc()
	}
	attrs := []any{
		slog.String("event", "delivery_settled"),
		slog.String("state", string(out.State)),
		slog.String("outcome", out.Label),
	}
	if out.Category != "" {
		attrs = append(attrs, slog.String("category", string(out.Category)))
	}
	log.Info("delivery settled", attrs...)
	return broker.Ack
}

// terminalLabel maps an already-terminal state to its metric label.
func terminalLabel(s jobs.State) string {
	switch s {
	case jobs.StateDelivered:
		return "delivered"
	case jobs.StateUncertain:
		return "uncertain"
	default:
		return "held"
	}
}

// categoryOf reports a job's recorded category, or the empty string.
func categoryOf(job ledger.Job) string {
	if job.FailureCategory == nil {
		return ""
	}
	return *job.FailureCategory
}
