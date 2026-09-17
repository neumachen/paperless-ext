// Package renamer is the renamer application: an independently scalable
// consumer of the work queue.
//
// A delivery is taken through the real path: decode, durable ownership record,
// normalize, verified working copy, exclusive destination reservation, atomic
// publication, durable receipt, acknowledge. Every outcome is committed before
// the delivery is settled, and no path overwrites a file. See pipeline.go for
// the ordering that makes an interrupted publication recoverable.
package renamer

import (
	"context"
	"log/slog"

	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/app"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/config"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/grpcapi"
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

	// A dry run must not consume the work queue: previewed work has to remain
	// processable afterwards, and an acknowledgement would destroy it. The
	// consumer is therefore not started at all in that mode.
	var proc *Processor
	if a.cfg.DryRun {
		sup.Add("preview", NewPreviewer(a.base, a.cfg).Run)
	} else {
		proc = NewProcessor(a.base, a.cfg)
		sup.Add("consumer", proc.Run)
	}

	log.Info("renamer configured",
		slog.String("event", "renamer_configured"),
		slog.String("policy_version", a.cfg.Policy.Identity),
		slog.Int("concurrency", a.cfg.Concurrency),
		slog.Int("prefetch", a.cfg.Prefetch),
		slog.String("queue", a.cfg.Broker.Queue))
	if a.cfg.Faults.Enabled() {
		log.Error("INJECTED FAULT POINTS ARE ARMED: this instance will deliberately "+
			"stop in the middle of publishing a document. Never run a real deployment like this.",
			slog.String("event", "fault_points_armed"),
			slog.Any("state", a.cfg.Faults.Names()))
	}

	if a.cfg.GRPCAddr != "" {
		api := grpcapi.New(a.cfg.GRPCAddr, grpcapi.Deps{
			Common:    a.cfg.Common,
			Effective: a.cfg.Effective(),
			Ledger:    a.base.Ledger,
			Health:    a.base.Health,
			Metrics:   a.base.Metrics,
			Logger:    log,
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
		inFlight := 0
		if proc != nil {
			inFlight = proc.InFlight()
		}
		log.Info("readiness withdrawn for drain",
			slog.String("event", "readiness_withdrawn"),
			slog.Bool("ready", false),
			slog.Int("count", inFlight))
	})
	sup.OnClose(func(ctx context.Context) { a.base.Close(ctx) })

	if graceful := sup.Run(ctx); !graceful {
		return 1
	}
	return 0
}
