// Package telemetry owns the Prometheus registry and every metric both
// applications expose.
//
// Cardinality rule: every label used here is drawn from a closed set that is
// enumerated in code. Job identifiers, file names, paths and fingerprints are
// never label values. The registry is constructed with the full label space
// pre-initialised so a scrape shows a zero series rather than a missing one.
package telemetry

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/buildinfo"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/jobs"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/logging"
)

// Dependency names used as the sole values of the "dependency" label.
const (
	DepPostgresPrimary = "postgres_primary"
	DepPostgresReplica = "postgres_replica"
	DepRabbitMQ        = "rabbitmq"
	DepStorage         = "storage"
)

func dependencies() []string {
	return []string{DepPostgresPrimary, DepPostgresReplica, DepRabbitMQ, DepStorage}
}

func storageRoles() []string {
	return []string{"incoming", "queued", "staging", "consume", "failed"}
}

// Metrics is the process metric set.
type Metrics struct {
	Registry *prometheus.Registry

	BuildInfo     *prometheus.GaugeVec
	Ready         prometheus.Gauge
	DependencyUp  *prometheus.GaugeVec
	StorageUp     *prometheus.GaugeVec
	StorageStatus *prometheus.GaugeVec

	SchemaUsable      prometheus.Gauge
	SchemaInstalled   prometheus.Gauge
	SchemaExpected    prometheus.Gauge
	ReplicaInRecovery prometheus.Gauge

	JobsByState       *prometheus.GaugeVec
	OldestPendingAge  prometheus.Gauge
	AccountingRuns    *prometheus.CounterVec
	DispatchAttempts  *prometheus.CounterVec
	DispatchConfirmed prometheus.Counter
	DispatchReclaimed prometheus.Counter

	Deliveries       *prometheus.CounterVec
	Redeliveries     prometheus.Counter
	DeliveryInFlight prometheus.Gauge
	DeliverySeconds  prometheus.Histogram
	ConsumerUp       prometheus.Gauge
	ConcurrencyLimit prometheus.Gauge
	PrefetchLimit    prometheus.Gauge

	BrokerReconnects prometheus.Counter
	LedgerErrors     *prometheus.CounterVec
}

// New builds the registry for one process.
func New(app, instance string) *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	m := &Metrics{Registry: reg}

	m.BuildInfo = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "fn_build_info",
		Help: "Build identity of the running application; always 1.",
	}, []string{"application", "version", "revision", "go_version", "policy_version", "contract_version", "source_digest"})

	m.Ready = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "fn_ready",
		Help: "1 when the application reports itself ready, 0 otherwise.",
	})

	m.DependencyUp = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "fn_dependency_up",
		Help: "1 when the named dependency answered its last probe, 0 otherwise.",
	}, []string{"dependency"})

	m.StorageUp = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "fn_storage_root_available",
		Help: "1 when the configured storage root for the role is present and usable.",
	}, []string{"role"})

	m.StorageStatus = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "fn_storage_root_status",
		Help: "1 for the current probe status of each storage role. Unavailable storage is reported distinctly from an empty directory.",
	}, []string{"role", "status"})

	m.SchemaUsable = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "fn_ledger_schema_usable",
		Help: "1 when the ledger schema is present and at least the version this build expects.",
	})

	m.SchemaInstalled = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "fn_ledger_schema_version_installed",
		Help: "Highest migration version recorded in the database, or 0 when none is.",
	})

	m.SchemaExpected = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "fn_ledger_schema_version_expected",
		Help: "Highest migration version embedded in this build.",
	})

	m.ReplicaInRecovery = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "fn_postgres_replica_in_recovery",
		Help: "1 while the configured standby is still in recovery. 0 means it answered but has been promoted, so it is no longer replicating.",
	})

	m.JobsByState = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "fn_jobs",
		Help: "Durable job count per ledger state, as last observed by the accounting worker.",
	}, []string{"state"})

	m.OldestPendingAge = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "fn_oldest_pending_dispatch_age_seconds",
		Help: "Age of the oldest job still owing a broker publication.",
	})

	m.AccountingRuns = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "fn_accounting_runs_total",
		Help: "Completion-accounting passes, by outcome.",
	}, []string{"outcome"})

	m.DispatchAttempts = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "fn_dispatch_attempts_total",
		Help: "Broker publication attempts by the watcher, by outcome.",
	}, []string{"outcome"})

	m.DispatchConfirmed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "fn_dispatch_confirmed_total",
		Help: "Publications acknowledged by a broker publisher confirm.",
	})

	m.DispatchReclaimed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "fn_dispatch_reclaimed_total",
		Help: "Stranded dispatch claims returned to pending_dispatch by the reaper.",
	})

	m.Deliveries = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "fn_deliveries_total",
		Help: "Broker deliveries handled by a renamer, by terminal outcome for the delivery.",
	}, []string{"outcome"})

	m.Redeliveries = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "fn_redeliveries_total",
		Help: "Deliveries the broker marked as redelivered.",
	})

	m.DeliveryInFlight = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "fn_deliveries_in_flight",
		Help: "Deliveries currently being processed by this instance.",
	})

	m.DeliverySeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "fn_delivery_duration_seconds",
		Help:    "Wall time from delivery receipt to acknowledgement decision.",
		Buckets: prometheus.DefBuckets,
	})

	m.ConsumerUp = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "fn_consumer_up",
		Help: "1 while this instance holds an active broker consumer.",
	})

	// The configured bounds, exposed so an assertion about "within its bound"
	// can read the bound from the instance it is checking rather than from
	// whatever configuration the observer happens to have loaded. Both read 0
	// on the watcher, which hosts no consumer.
	m.ConcurrencyLimit = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "fn_renamer_concurrency_limit",
		Help: "Configured maximum simultaneous in-flight deliveries for this instance. 0 on the watcher.",
	})

	m.PrefetchLimit = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "fn_renamer_prefetch_limit",
		Help: "Configured AMQP prefetch window for this instance. 0 on the watcher.",
	})

	m.BrokerReconnects = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "fn_broker_reconnects_total",
		Help: "Broker connection establishments after an initial successful connection.",
	})

	m.LedgerErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "fn_ledger_errors_total",
		Help: "Durable-store failures by coarse error kind.",
	}, []string{"error_kind"})

	reg.MustRegister(
		m.BuildInfo, m.Ready, m.DependencyUp, m.StorageUp, m.StorageStatus,
		m.SchemaUsable, m.SchemaInstalled, m.SchemaExpected, m.ReplicaInRecovery,
		m.JobsByState, m.OldestPendingAge, m.AccountingRuns,
		m.DispatchAttempts, m.DispatchConfirmed, m.DispatchReclaimed,
		m.Deliveries, m.Redeliveries, m.DeliveryInFlight, m.DeliverySeconds,
		m.ConsumerUp, m.ConcurrencyLimit, m.PrefetchLimit,
		m.BrokerReconnects, m.LedgerErrors,
	)

	m.BuildInfo.WithLabelValues(
		app, buildinfo.Version, buildinfo.Revision, buildinfo.GoVersion(),
		buildinfo.PolicyVersion, contractVersionLabel(), buildinfo.SourceDigest,
	).Set(1)

	m.preinitialise()
	return m
}

// preinitialise materialises the whole closed label space at zero so a scrape
// distinguishes "nothing happened yet" from "this series does not exist".
func (m *Metrics) preinitialise() {
	for _, d := range dependencies() {
		m.DependencyUp.WithLabelValues(d).Set(0)
	}
	for _, r := range storageRoles() {
		m.StorageUp.WithLabelValues(r).Set(0)
		for _, s := range StorageStatuses() {
			m.StorageStatus.WithLabelValues(r, s).Set(0)
		}
	}
	for _, s := range jobs.States() {
		m.JobsByState.WithLabelValues(string(s)).Set(0)
	}
	for _, o := range []string{"confirmed", "unconfirmed", "returned", "publish_error"} {
		m.DispatchAttempts.WithLabelValues(o)
	}
	for _, o := range DeliveryOutcomes() {
		m.Deliveries.WithLabelValues(o)
	}
	for _, o := range []string{"ok", "error"} {
		m.AccountingRuns.WithLabelValues(o)
	}
	for _, k := range logging.ErrorKinds() {
		m.LedgerErrors.WithLabelValues(k)
	}
	m.Ready.Set(0)
	m.SchemaUsable.Set(0)
	m.SchemaInstalled.Set(0)
	m.SchemaExpected.Set(0)
	m.ReplicaInRecovery.Set(0)
	m.ConsumerUp.Set(0)
	m.ConcurrencyLimit.Set(0)
	m.PrefetchLimit.Set(0)
	m.DeliveryInFlight.Set(0)
	m.OldestPendingAge.Set(0)
}

// DeliveryOutcomes is the closed set of per-delivery outcomes.
//
// "held" is a durably recorded hold requiring intervention. This increment
// never emits a "delivered" outcome, because nothing is published to a
// consume directory yet.
func DeliveryOutcomes() []string {
	return []string{"held", "requeued", "dead_lettered", "rejected_unknown_job", "rejected_contract"}
}

// StorageStatuses is the closed set of storage probe results.
func StorageStatuses() []string {
	return []string{"ok", "empty", "missing", "not_a_directory", "permission_denied", "not_writable", "probe_error"}
}

func contractVersionLabel() string {
	switch jobs.ContractVersion {
	case 1:
		return "1"
	default:
		return "unknown"
	}
}
