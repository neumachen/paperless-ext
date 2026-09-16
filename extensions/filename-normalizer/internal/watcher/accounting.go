package watcher

import (
	"context"
	"log/slog"
	"time"

	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/app"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/config"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/logging"
)

// Accountant is the completion-accounting worker.
//
// It derives the reported counts from the durable ledger, never from broker
// acknowledgements: an acknowledgement settles a delivery, it does not create
// a completion record. Anything this worker reports is therefore backed by a
// committed row.
type Accountant struct {
	base *app.Base
	cfg  config.WatcherConfig
	log  *slog.Logger
}

// NewAccountant builds the accounting worker.
func NewAccountant(base *app.Base, cfg config.WatcherConfig) *Accountant {
	return &Accountant{base: base, cfg: cfg, log: base.Log.With(slog.String("component", "accounting"))}
}

// Run aggregates the ledger on a fixed interval.
func (a *Accountant) Run(ctx context.Context) {
	t := time.NewTicker(a.cfg.AccountingInterval)
	defer t.Stop()
	a.pass(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			a.pass(ctx)
		}
	}
}

func (a *Accountant) pass(ctx context.Context) {
	counts, err := a.base.Ledger.Snapshot(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		a.base.Metrics.AccountingRuns.WithLabelValues("error").Inc()
		a.base.Metrics.LedgerErrors.WithLabelValues(logging.ErrorKind(err)).Inc()
		a.log.Error("accounting pass failed",
			slog.String("event", "accounting_failed"),
			slog.String("dependency", "postgres_primary"),
			slog.String("error_kind", logging.ErrorKind(err)))
		return
	}

	for state, n := range counts.ByState {
		a.base.Metrics.JobsByState.WithLabelValues(string(state)).Set(float64(n))
	}
	a.base.Metrics.OldestPendingAge.Set(counts.OldestPendingAge.Seconds())
	a.base.Metrics.AccountingRuns.WithLabelValues("ok").Inc()

	a.log.Debug("accounting pass complete",
		slog.String("event", "accounting_pass"),
		slog.Int("count", counts.Total),
		slog.Int64("oldest_pending_age_ms", counts.OldestPendingAge.Milliseconds()))
}
