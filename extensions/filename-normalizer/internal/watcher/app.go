// Package watcher is the watcher application.
//
// It hosts three workers:
//
//   - dispatch: publishes durable jobs that still owe a broker publication,
//     and returns stranded claims to the pending set.
//
//   - accounting: aggregates the ledger into the exposed metrics.
//
//   - discovery: registers eligible completed submissions from the incoming
//     root, and reconciles work that arrived while this process was down.
package watcher

import (
	"context"
	"log/slog"

	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/app"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/config"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/grpcapi"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/runtime"
)

// App is the assembled watcher.
type App struct {
	cfg  config.WatcherConfig
	base *app.Base
}

// New assembles the watcher.
func New(ctx context.Context, cfg config.WatcherConfig) (*App, error) {
	base, err := app.NewBase(ctx, cfg.Common)
	if err != nil {
		return nil, err
	}
	return &App{cfg: cfg, base: base}, nil
}

// Base exposes the shared runtime.
func (a *App) Base() *app.Base { return a.base }

// Run starts the watcher and blocks until termination.
func (a *App) Run(ctx context.Context) int {
	log := a.base.Log

	if err := a.base.StartHealth(ctx); err != nil {
		return runtime.LogFatal(log, "http_listen_failed", err)
	}

	sup := runtime.New(log, a.cfg.ShutdownTimeout)

	// Migrations run in their own worker so a database that is down at startup
	// produces a not-ready process that recovers, instead of an exit.
	if a.cfg.Database.ApplyMigrations {
		sup.Add("migrator", func(ctx context.Context) { a.runMigrations(ctx) })
	}

	// Infrastructure: cancelled only after every service worker has drained,
	// so a dispatcher finishing its last publication still has a connection.
	sup.AddInfrastructure("broker", a.base.Conn.Run)

	dispatcher := NewDispatcher(a.base, a.cfg)
	sup.Add("dispatch", dispatcher.Run)

	accountant := NewAccountant(a.base, a.cfg)
	sup.Add("accounting", accountant.Run)

	discoverer := NewDiscoverer(a.base, a.cfg)
	sup.Add("discovery", discoverer.Run)

	if a.cfg.GRPCAddr != "" {
		api := grpcapi.New(a.cfg.GRPCAddr, grpcapi.Deps{
			Common:           a.cfg.Common,
			Effective:        a.cfg.Effective(),
			Ledger:           a.base.Ledger,
			Health:           a.base.Health,
			Metrics:          a.base.Metrics,
			Logger:           log,
			DiscoveryEnabled: a.cfg.Discovery.Enabled,
		})
		sup.Add("grpc", func(ctx context.Context) {
			if err := api.Serve(ctx); err != nil {
				log.Error("the grpc api stopped with an error",
					slog.String("event", "grpc_failed"),
					slog.String("error_kind", "unclassified"))
			}
		})
	}

	sup.OnDrain(func(context.Context) {
		a.base.Health.SetNotReady("shutting_down")
		log.Info("readiness withdrawn for drain",
			slog.String("event", "readiness_withdrawn"),
			slog.Bool("ready", false))
	})
	sup.OnClose(func(ctx context.Context) { a.base.Close(ctx) })

	if graceful := sup.Run(ctx); !graceful {
		return 1
	}
	return 0
}
