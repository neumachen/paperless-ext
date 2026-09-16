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
}

// NewProcessor builds the consumer worker.
func NewProcessor(base *app.Base, cfg config.RenamerConfig) *Processor {
	log := base.Log.With(slog.String("component", "consumer"))
	p := &Processor{base: base, cfg: cfg, log: log}
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
		// delivery must go back to the broker; x-delivery-limit bounds it.
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

	if job.State == jobs.StateHeld {
		// A redelivery of an already-held job. The hold is already durable, so
		// the delivery is settled without repeating the outcome.
		p.base.Metrics.Deliveries.WithLabelValues("held").Inc()
		log.Info("delivery settled against an existing hold",
			slog.String("event", "delivery_settled"),
			slog.String("state", string(job.State)),
			slog.String("outcome", "held"),
			slog.String("category", string(jobs.CategoryNormalizationUnimplemented)))
		return broker.Ack
	}

	// This is where normalization, reservation and publication will run. They
	// are not implemented, so the only honest outcome is a durably recorded
	// hold requiring intervention. Nothing claims that a document was
	// delivered, and no filesystem state is changed.
	if err := p.base.Ledger.RecordHold(ctx, msg.JobID, jobs.CategoryNormalizationUnimplemented, msg.Attempt); err != nil {
		p.base.Metrics.Deliveries.WithLabelValues("requeued").Inc()
		p.base.Metrics.LedgerErrors.WithLabelValues(logging.ErrorKind(err)).Inc()
		log.Error("could not record the outcome durably; returning the delivery and pausing consumption",
			slog.String("event", "delivery_requeued"),
			slog.String("dependency", "postgres_primary"),
			slog.String("category", string(jobs.CategoryLedgerUnavailable)),
			slog.String("error_kind", logging.ErrorKind(err)))
		p.consumer.RequestDetach()
		return broker.NackRequeue
	}

	p.base.Metrics.Deliveries.WithLabelValues("held").Inc()
	log.Info("delivery settled",
		slog.String("event", "delivery_settled"),
		slog.String("state", string(jobs.StateHeld)),
		slog.String("outcome", "held"),
		slog.String("category", string(jobs.CategoryNormalizationUnimplemented)))
	return broker.Ack
}
