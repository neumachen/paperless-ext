// Package renamer is the renamer application: an independently scalable
// consumer of the work queue.
//
// Scope note for this increment: normalization is not implemented. A delivery
// is therefore taken through the real path — decode, durable ownership record,
// durable outcome, acknowledge — and the outcome recorded is an honest hold
// with the category normalization_unimplemented. No file is touched, no
// destination is reserved, and no delivery is ever claimed.
package renamer

import (
	"context"
	"log/slog"

	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/app"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/config"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/runtime"
)

// App is the assembled renamer.
type App struct {
	cfg  config.RenamerConfig
	base *app.Base
}

// New assembles the renamer.
func New(ctx context.Context, cfg config.RenamerConfig) (*App, error) {
	base, err := app.NewBase(ctx, cfg.Common)
	if err != nil {
		return nil, err
	}
	return &App{cfg: cfg, base: base}, nil
}

// Base exposes the shared runtime.
func (a *App) Base() *app.Base { return a.base }

// Run starts the renamer and blocks until termination.
func (a *App) Run(ctx context.Context) int {
	log := a.base.Log

	if err := a.base.StartHealth(ctx); err != nil {
		return runtime.LogFatal(log, "http_listen_failed", err)
	}

	sup := runtime.New(log, a.cfg.ShutdownTimeout)
	// Infrastructure: the broker connection must outlive the consumer's drain,
	// otherwise a delivery still being settled loses the channel it has to
	// acknowledge on.
	sup.AddInfrastructure("broker", a.base.Conn.Run)

	proc := NewProcessor(a.base, a.cfg)
	sup.Add("consumer", proc.Run)

	log.Info("renamer configured",
		slog.String("event", "renamer_configured"),
		slog.Int("concurrency", a.cfg.Concurrency),
		slog.Int("prefetch", a.cfg.Prefetch),
		slog.String("queue", a.cfg.Broker.Queue))
	log.Warn("filename normalization is not implemented in this build",
		slog.String("event", "normalization_unimplemented"),
		slog.String("category", "normalization_unimplemented"))

	sup.OnDrain(func(context.Context) {
		a.base.Health.SetNotReady("shutting_down")
		log.Info("readiness withdrawn for drain",
			slog.String("event", "readiness_withdrawn"),
			slog.Bool("ready", false),
			slog.Int("count", proc.InFlight()))
	})
	sup.OnClose(func(ctx context.Context) { a.base.Close(ctx) })

	if graceful := sup.Run(ctx); !graceful {
		return 1
	}
	return 0
}
