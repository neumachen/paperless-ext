// Package app wires the pieces both executables share: logger, metrics,
// durable store, broker supervisor and the health surface.
package app

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"time"

	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/broker"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/buildinfo"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/config"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/health"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/ledger"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/logging"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/storage"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/telemetry"
)

// Base is the shared runtime context of one process.
type Base struct {
	Cfg     config.Common
	Log     *slog.Logger
	Metrics *telemetry.Metrics
	Ledger  *ledger.Ledger
	Conn    *broker.Connection
	Health  *health.Server
	Roots   []storage.Root
}

// NewBase builds the shared runtime. It does not connect to anything: the
// readiness probes establish and report real connectivity, so a dependency
// that is down at startup produces a running, not-ready process rather than a
// crash loop.
func NewBase(ctx context.Context, cfg config.Common) (*Base, error) {
	level, ok := logging.ParseLevel(cfg.LogLevel)
	if !ok {
		return nil, errors.New("unsupported log level")
	}
	log := logging.New(os.Stdout, logging.Options{
		Level:       level,
		Application: string(cfg.Application),
		Instance:    cfg.Instance,
	})

	metrics := telemetry.New(string(cfg.Application), cfg.Instance, cfg.Policy.Identity)

	led, err := ledger.Open(ctx, ledger.Options{
		Config:    cfg.Database,
		Logger:    log.With(slog.String("component", "ledger")),
		Actor:     cfg.Instance,
		OpTimeout: 10 * time.Second,
	})
	if err != nil {
		return nil, err
	}

	conn := broker.Dial(broker.Options{
		Config: cfg.Broker,
		Logger: log.With(slog.String("component", "broker")),
		Name:   string(cfg.Application) + "/" + cfg.Instance,
		OnReconnect: func() {
			metrics.BrokerReconnects.Inc()
		},
		OnUp: func(up bool) {
			metrics.DependencyUp.WithLabelValues(telemetry.DepRabbitMQ).Set(boolGauge(up))
		},
	})

	roots := make([]storage.Root, 0, 5)
	for _, r := range cfg.Storage.RolesFor(cfg.Application) {
		roots = append(roots, storage.Root{Role: r.Name, Path: r.Path, WriteRequired: r.WriteRequired})
	}

	b := &Base{
		Cfg: cfg, Log: log, Metrics: metrics,
		Ledger: led, Conn: conn, Roots: roots,
	}

	b.Health = health.New(health.Options{
		Addr:           cfg.HTTPAddr,
		Application:    string(cfg.Application),
		Instance:       cfg.Instance,
		PolicyIdentity: cfg.Policy.Identity,
		Logger:         log.With(slog.String("component", "http")),
		Registry:       metrics.Registry,
		ProbeTimeout:   3 * time.Second,
		Checks:         b.checks(),
		OnReadiness: func(ready bool, _ []health.State) {
			metrics.Ready.Set(boolGauge(ready))
		},
	})

	log.Info("configuration loaded",
		slog.String("event", "config_loaded"),
		slog.String("version", buildinfo.Version),
		slog.String("revision", buildinfo.Revision),
		slog.String("build_date", buildinfo.BuildDate),
		slog.String("go_version", buildinfo.GoVersion()),
		slog.String("policy_version", cfg.Policy.Identity),
		slog.Int("pid", os.Getpid()),
		slog.Any("state", summaryAttrs(cfg)))

	return b, nil
}

// summaryAttrs converts the credential-free configuration summary into log
// attributes. Every key it produces is on the logger's allow-list.
func summaryAttrs(cfg config.Common) []slog.Attr {
	sum := cfg.Summary()
	keys := []string{
		"host", "port", "database", "replica_host", "replica_port",
		"replica_required", "tls", "exchange", "queue", "routing_key",
		"dlx", "dead_letter_queue", "vhost", "max_attempts",
		"shutdown_timeout_ms", "addr",
	}
	out := make([]slog.Attr, 0, len(keys))
	for _, k := range keys {
		if v, ok := sum[k]; ok {
			out = append(out, slog.Any(k, v))
		}
	}
	return out
}

func (b *Base) checks() []health.Check {
	checks := []health.Check{{
		Name:     telemetry.DepPostgresPrimary,
		Required: true,
		Probe: func(ctx context.Context) (bool, string) {
			if err := b.Ledger.PingPrimary(ctx); err != nil {
				b.Metrics.DependencyUp.WithLabelValues(telemetry.DepPostgresPrimary).Set(0)
				b.Metrics.SchemaUsable.Set(0)
				return false, logging.ErrorKind(err)
			}
			b.Metrics.DependencyUp.WithLabelValues(telemetry.DepPostgresPrimary).Set(1)

			// Reachability is not readiness. A database whose migration never
			// ran would otherwise let this instance advertise itself as ready
			// and then fail on its first durable write.
			state, err := b.Ledger.SchemaReady(ctx)
			b.Metrics.SchemaInstalled.Set(float64(state.InstalledVersion))
			b.Metrics.SchemaExpected.Set(float64(state.ExpectedVersion))
			if err != nil {
				b.Metrics.SchemaUsable.Set(0)
				category := "schema_unusable"
				if state.InstalledVersion > 0 && state.InstalledVersion < state.ExpectedVersion {
					category = "schema_outdated"
				} else if state.InstalledVersion == 0 {
					category = "schema_missing"
				}
				b.Log.Error("the ledger schema is not usable",
					slog.String("event", "schema_unusable"),
					slog.String("dependency", telemetry.DepPostgresPrimary),
					slog.String("category", category),
					slog.Int("count", state.InstalledVersion),
					slog.String("error_kind", logging.ErrorKind(err)))
				return false, category
			}
			b.Metrics.SchemaUsable.Set(1)
			return true, ""
		},
	}, {
		Name:     telemetry.DepRabbitMQ,
		Required: true,
		Probe: func(_ context.Context) (bool, string) {
			up := b.Conn.Connected()
			b.Metrics.DependencyUp.WithLabelValues(telemetry.DepRabbitMQ).Set(boolGauge(up))
			if !up {
				return false, "connection_lost"
			}
			return true, ""
		},
	}}

	if b.Ledger.HasReplica() {
		checks = append(checks, health.Check{
			Name:     telemetry.DepPostgresReplica,
			Required: b.Cfg.Database.ReplicaRequired,
			Probe: func(ctx context.Context) (bool, string) {
				inRecovery, err := b.Ledger.PingReplica(ctx)
				b.Metrics.DependencyUp.WithLabelValues(telemetry.DepPostgresReplica).Set(boolGauge(err == nil))
				if err != nil {
					b.Metrics.ReplicaInRecovery.Set(0)
					return false, logging.ErrorKind(err)
				}
				b.Metrics.ReplicaInRecovery.Set(boolGauge(inRecovery))
				if !inRecovery {
					// A standby that left recovery has been promoted. It still
					// answers queries, so the check passes, but the category is
					// retained and reported: the health server no longer
					// discards a category just because the probe succeeded.
					b.Log.Warn("the configured standby has left recovery and is no longer replicating",
						slog.String("event", "replica_promoted"),
						slog.String("dependency", telemetry.DepPostgresReplica),
						slog.String("category", "promoted_not_in_recovery"),
						slog.Bool("in_recovery", false))
					return true, "promoted_not_in_recovery"
				}
				return true, ""
			},
		})
	}

	checks = append(checks, health.Check{
		Name:     telemetry.DepStorage,
		Required: b.Cfg.Storage.Required,
		Probe: func(_ context.Context) (bool, string) {
			report := storage.Probe(b.Roots)
			worst := ""
			for _, res := range report.Results {
				b.Metrics.StorageUp.WithLabelValues(res.Role).Set(boolGauge(res.Status.Available()))
				for _, s := range telemetry.StorageStatuses() {
					v := 0.0
					if string(res.Status) == s {
						v = 1
					}
					b.Metrics.StorageStatus.WithLabelValues(res.Role, s).Set(v)
				}
				if !res.Status.Available() && worst == "" {
					worst = res.Role + ":" + string(res.Status)
					b.Log.Error("configured storage root is unavailable",
						slog.String("event", "storage_unavailable"),
						slog.String("storage_role", res.Role),
						slog.String("storage_status", string(res.Status)))
				}
			}
			available := report.Available()
			b.Metrics.DependencyUp.WithLabelValues(telemetry.DepStorage).Set(boolGauge(available))
			if !available {
				return false, worst
			}
			return true, ""
		},
	})

	return checks
}

// StartHealth binds the HTTP surface and begins probing.
func (b *Base) StartHealth(ctx context.Context) error {
	return b.Health.Start(ctx, 2*time.Second)
}

// Close releases the shared dependencies.
func (b *Base) Close(ctx context.Context) {
	_ = b.Health.Shutdown(ctx)
	b.Conn.Shutdown()
	b.Ledger.Close()
}

func boolGauge(v bool) float64 {
	if v {
		return 1
	}
	return 0
}
