//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/ledger"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/telemetry"
)

// FN-F004 — health and readiness must not report success while the
// application is unusable.

// TestF4SchemaReadinessRequiresAUsableLedger asserts that a reachable database
// is not treated as readiness.
//
// The orchestrator starts a renamer pointed at the "postgres" maintenance
// database: it exists, it authenticates, and it has never had a migration
// applied, with migrations disabled for that instance. Connectivity alone
// would let it advertise itself as ready and then fail on its first durable
// write, so readiness must be false with a schema category.
func TestF4SchemaReadinessRequiresAUsableLedger(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseSchemaFault)

	var (
		r      Readiness
		status int
		raw    []byte
	)
	err := waitForErr(120*time.Second, func() error {
		var gerr error
		r, status, raw, gerr = GetReadiness(e.NoSchemaURL)
		if gerr != nil {
			return fmt.Errorf("%s /readyz: %w", e.NoSchemaURL, gerr)
		}
		if _, _, found := r.Check(telemetry.DepPostgresPrimary); !found {
			return fmt.Errorf("%s has not reported a postgres_primary check yet", e.NoSchemaURL)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("%v", err)
	}

	// The process must be alive and reporting a fault, not crashed.
	if _, hstatus, herr := GetHealth(e.NoSchemaURL); herr != nil || hstatus != http.StatusOK {
		t.Errorf("%s /healthz: status=%d err=%v; an unusable schema should not kill the process",
			e.NoSchemaURL, hstatus, herr)
	}

	if r.Ready {
		t.Fatalf("an application whose ledger schema does not exist claimed readiness: %s", raw)
	}
	if status != http.StatusServiceUnavailable {
		t.Errorf("%s returned status %d while not ready, expected 503", e.NoSchemaURL, status)
	}

	ok, category, _ := r.Check(telemetry.DepPostgresPrimary)
	if ok {
		t.Fatalf("the postgres_primary check passed against a database with no ledger schema: %s", raw)
	}
	if !strings.HasPrefix(category, "schema_") {
		t.Errorf("the failure category is %q; an unusable schema must be reported as such, not as a connectivity fault", category)
	}
	t.Logf("the no-schema renamer reports postgres_primary category %q", category)

	// The distinction must also be visible to a scraper: the database is
	// reachable, and it is specifically the schema that is unusable.
	body, merr := GetMetrics(e.NoSchemaURL)
	if merr != nil {
		t.Fatalf("%s /metrics: %v", e.NoSchemaURL, merr)
	}
	if v := requireMetric(t, body, "fn_ledger_schema_usable", nil); v != 0 {
		t.Errorf("fn_ledger_schema_usable is %v against a database with no ledger schema", v)
	}
	if v := requireMetric(t, body, "fn_ledger_schema_version_installed", nil); v != 0 {
		t.Errorf("fn_ledger_schema_version_installed is %v, expected 0", v)
	}
	if v := requireMetric(t, body, "fn_ledger_schema_version_expected", nil); v < 1 {
		t.Errorf("fn_ledger_schema_version_expected is %v; the build reports no expected version", v)
	}
	if v := requireMetric(t, body, "fn_ready", nil); v != 0 {
		t.Errorf("fn_ready is %v while the schema is unusable", v)
	}
	// A consumer must not be attached: it would take deliveries it cannot settle.
	if v := requireMetric(t, body, "fn_consumer_up", nil); v != 0 {
		t.Errorf("fn_consumer_up is %v while the ledger schema is unusable", v)
	}

	e.WriteEvidence(t, "f4-schema-readiness.json", raw)
	e.WriteEvidence(t, "f4-schema-readiness-metrics.txt", []byte(
		describeSamples(body, "fn_ledger_schema_usable")+
			describeSamples(body, "fn_ledger_schema_version_installed")+
			describeSamples(body, "fn_ledger_schema_version_expected")+
			describeSamples(body, "fn_dependency_up")))
}

// TestF4HealthySchemaIsReportedUsable is the positive counterpart, so the
// check above cannot pass merely by always failing.
func TestF4HealthySchemaIsReportedUsable(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseBaseline)
	led := e.Ledger(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	expected, err := ledger.ExpectedSchemaVersion()
	if err != nil {
		t.Fatalf("read the expected schema version: %v", err)
	}
	state, err := led.SchemaReady(ctx)
	if err != nil {
		t.Fatalf("the ledger schema is not usable against the healthy cluster: %v", err)
	}
	if state.InstalledVersion < expected {
		t.Errorf("installed schema version %d is below the expected %d", state.InstalledVersion, expected)
	}
	if !state.JobsPresent || !state.EventsPresent {
		t.Errorf("a required relation is missing: jobs=%t job_events=%t", state.JobsPresent, state.EventsPresent)
	}

	for _, url := range append([]string{e.WatcherURL}, e.RenamerURLs...) {
		body, merr := GetMetrics(url)
		if merr != nil {
			t.Fatalf("%s /metrics: %v", url, merr)
		}
		if v := requireMetric(t, body, "fn_ledger_schema_usable", nil); v != 1 {
			t.Errorf("%s reports fn_ledger_schema_usable=%v against the healthy cluster", url, v)
		}
		if v := requireMetric(t, body, "fn_ledger_schema_version_installed", nil); int(v) != state.InstalledVersion {
			t.Errorf("%s reports installed schema version %v, the database says %d", url, v, state.InstalledVersion)
		}
	}
	t.Logf("schema usable: installed=%d expected=%d", state.InstalledVersion, expected)
}

// TestF4PromotedStandbyIsReportedNotDiscarded asserts the promoted-standby
// category survives to the readiness document.
//
// The health server used to clear a check's category whenever the check
// passed, which silently discarded exactly this case: a promoted standby still
// answers queries, so the check succeeds while carrying information an
// operator must see.
func TestF4PromotedStandbyIsReportedNotDiscarded(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseBaseline)

	// In the healthy steady state the standby is in recovery, the check
	// passes, and no category is reported.
	for _, url := range append([]string{e.WatcherURL}, e.RenamerURLs...) {
		r, _, raw, err := GetReadiness(url)
		if err != nil {
			t.Fatalf("%s /readyz: %v", url, err)
		}
		ok, category, found := r.Check(telemetry.DepPostgresReplica)
		if !found {
			t.Fatalf("%s reports no postgres_replica check: %s", url, raw)
		}
		if !ok {
			t.Errorf("%s reports the standby not ok in the healthy steady state (category %q)", url, category)
		}
		if category != "" {
			t.Errorf("%s reports category %q for a healthy in-recovery standby, expected none", url, category)
		}
		body, merr := GetMetrics(url)
		if merr != nil {
			t.Fatalf("%s /metrics: %v", url, merr)
		}
		if v := requireMetric(t, body, "fn_postgres_replica_in_recovery", nil); v != 1 {
			t.Errorf("%s reports fn_postgres_replica_in_recovery=%v for an in-recovery standby", url, v)
		}
	}

	// The promotion itself is exercised by the separate failover script, which
	// records an application readiness document taken while the standby was
	// promoted. That evidence is checked here when present, and its absence is
	// reported rather than passed over.
	raw, ok := e.OptionalState("promoted-readyz")
	if !ok {
		t.Skip("no promoted-standby readiness observation is present: run 'make test-failover', " +
			"which records one, to exercise this assertion")
	}
	var promoted Readiness
	if err := json.Unmarshal([]byte(raw), &promoted); err != nil {
		t.Fatalf("the recorded promoted-standby readiness document is not JSON: %v (%s)", err, raw)
	}
	pok, pcategory, pfound := promoted.Check(telemetry.DepPostgresReplica)
	if !pfound {
		t.Fatalf("the recorded document has no postgres_replica check: %s", raw)
	}
	if !pok {
		t.Errorf("the promoted standby was reported not ok; it still answers queries: %s", raw)
	}
	if pcategory != "promoted_not_in_recovery" {
		t.Errorf("the promoted standby reported category %q, expected promoted_not_in_recovery", pcategory)
	}
	e.WriteEvidence(t, "f4-promoted-standby-readyz.json", []byte(raw))
}
