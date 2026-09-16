package watcher

import (
	"context"
	"log/slog"
	"time"

	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/app"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/broker"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/config"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/jobs"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/logging"
)

// Dispatcher publishes durable jobs that still owe a broker publication.
//
// Ordering is deliberate. A job is claimed durably, then published, and only
// marked dispatched after a publisher confirm. A crash anywhere in between
// leaves the row recoverable: either it is still claimed and the reaper
// returns it, or it was published without being marked and the next pass
// republishes it under the same job identity. Duplicate messages are allowed;
// duplicate delivery must be safe, which is why the job ID never changes.
type Dispatcher struct {
	base *app.Base
	cfg  config.WatcherConfig
	pub  *broker.Publisher
	log  *slog.Logger
}

// NewDispatcher builds the dispatch worker.
func NewDispatcher(base *app.Base, cfg config.WatcherConfig) *Dispatcher {
	log := base.Log.With(slog.String("component", "dispatch"))
	return &Dispatcher{
		base: base,
		cfg:  cfg,
		pub:  broker.NewPublisher(base.Conn, broker.TopologyFromConfig(cfg.Broker), log, cfg.Broker.ConfirmTimeout),
		log:  log,
	}
}

// Run polls for pending work until the context is cancelled.
func (d *Dispatcher) Run(ctx context.Context) {
	defer d.pub.Close()
	t := time.NewTicker(d.cfg.DispatchInterval)
	defer t.Stop()
	reap := time.NewTicker(d.cfg.DispatchClaimMaxAge / 2)
	defer reap.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			d.pass(ctx)
		case <-reap.C:
			d.reap(ctx)
		}
	}
}

func (d *Dispatcher) pass(ctx context.Context) {
	// Publishing into a broker that is not connected would only produce
	// claimed-but-unpublished rows, so the pass is skipped until it returns.
	if !d.base.Conn.Connected() {
		return
	}

	claimed, err := d.base.Ledger.ClaimForDispatch(ctx, d.cfg.DispatchBatch)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		d.base.Metrics.LedgerErrors.WithLabelValues(logging.ErrorKind(err)).Inc()
		d.log.Error("could not claim work for dispatch",
			slog.String("event", "dispatch_claim_failed"),
			slog.String("dependency", "postgres_primary"),
			slog.String("error_kind", logging.ErrorKind(err)))
		return
	}
	if len(claimed) == 0 {
		return
	}

	for _, job := range claimed {
		if ctx.Err() != nil {
			return
		}
		d.publish(ctx, job.JobID, job.DispatchAttempts)
	}
}

func (d *Dispatcher) publish(ctx context.Context, jobID string, attempt int) {
	msg := jobs.Message{
		ContractVersion: jobs.ContractVersion,
		JobID:           jobID,
		Attempt:         attempt,
		EnqueuedAt:      time.Now().UTC(),
	}

	result, err := d.pub.Publish(ctx, msg)
	d.base.Metrics.DispatchAttempts.WithLabelValues(string(result)).Inc()

	if result == broker.PublishConfirmed {
		d.base.Metrics.DispatchConfirmed.Inc()
		if merr := d.base.Ledger.MarkDispatched(ctx, jobID, attempt); merr != nil {
			// The message is already on the broker. Failing to record that is
			// not a loss: the row stays claimed and the reaper returns it, so
			// the job is republished under the same identity.
			d.base.Metrics.LedgerErrors.WithLabelValues(logging.ErrorKind(merr)).Inc()
			d.log.Error("publication confirmed but could not be recorded",
				slog.String("event", "dispatch_record_failed"),
				slog.String("job_id", jobID),
				slog.Int("attempt", attempt),
				slog.String("error_kind", logging.ErrorKind(merr)))
			return
		}
		d.log.Info("job dispatched",
			slog.String("event", "job_dispatched"),
			slog.String("job_id", jobID),
			slog.Int("attempt", attempt),
			slog.String("state", string(jobs.StateDispatched)),
			slog.String("outcome", string(result)))
		return
	}

	d.log.Error("job publication did not confirm",
		slog.String("event", "dispatch_failed"),
		slog.String("job_id", jobID),
		slog.Int("attempt", attempt),
		slog.String("outcome", string(result)),
		slog.String("error_kind", logging.ErrorKind(err)))

	if rerr := d.base.Ledger.ReturnToPending(ctx, jobID, jobs.CategoryLedgerUnavailable, jobs.EventDispatchFailed); rerr != nil {
		d.base.Metrics.LedgerErrors.WithLabelValues(logging.ErrorKind(rerr)).Inc()
		d.log.Error("could not return an unconfirmed job to pending",
			slog.String("event", "dispatch_return_failed"),
			slog.String("job_id", jobID),
			slog.String("error_kind", logging.ErrorKind(rerr)))
	}
}

func (d *Dispatcher) reap(ctx context.Context) {
	n, err := d.base.Ledger.ReclaimStaleDispatch(ctx, d.cfg.DispatchClaimMaxAge)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		d.base.Metrics.LedgerErrors.WithLabelValues(logging.ErrorKind(err)).Inc()
		return
	}
	if n > 0 {
		d.base.Metrics.DispatchReclaimed.Add(float64(n))
	}
}
