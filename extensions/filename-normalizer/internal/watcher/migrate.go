package watcher

import (
	"context"
	"log/slog"
	"time"

	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/logging"
)

// runMigrations applies the embedded schema, retrying while the primary is
// unavailable. It never gives up silently: every failure is logged with a
// sanitized error kind and the process stays not-ready until it succeeds.
func (a *App) runMigrations(ctx context.Context) {
	log := a.base.Log.With(slog.String("component", "migrator"))
	backoff := time.Second
	const maxBackoff = 15 * time.Second

	for ctx.Err() == nil {
		err := a.base.Ledger.Migrate(ctx)
		if err == nil {
			v, verr := a.base.Ledger.SchemaVersion(ctx)
			if verr == nil {
				log.Info("schema is up to date",
					slog.String("event", "schema_ready"),
					slog.Int("count", v))
			}
			return
		}
		if ctx.Err() != nil {
			return
		}
		log.Error("schema migration failed; will retry",
			slog.String("event", "migration_retry"),
			slog.String("dependency", "postgres_primary"),
			slog.String("error_kind", logging.ErrorKind(err)),
			slog.Int64("interval_ms", backoff.Milliseconds()))

		t := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-t.C:
		}
		if backoff < maxBackoff {
			backoff *= 2
		}
	}
}
