//go:build integration

package integration

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/jobs"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/ledger"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/telemetry"
)

// F3 — real PostgreSQL integration against the cluster.
//
// Topology under test: one primary and one asynchronous streaming standby.
// These assertions use the applications' own ledger package, so a passing run
// exercises the code the applications ship. A TCP connection is never treated
// as evidence: every check below reads or writes real rows, or reads real
// replication state from the primary.

// registerSyntheticJob makes one durable job through the real ledger code.
func registerSyntheticJob(t *testing.T, led *ledger.Ledger, e *Env, label string) ledger.Job {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Synthetic content: generated here, never a real document.
	content := []byte(fmt.Sprintf("synthetic-document:%s:%s", e.RunID, label))
	sum := sha256.Sum256(content)
	size := int64(len(content))
	algo := "sha256"

	job, err := led.RegisterJob(ctx, ledger.RegisterInput{
		SourceRoot:      e.Cfg.Storage.Incoming,
		SourceName:      fmt.Sprintf("synthetic %s %s.pdf", e.RunID, label),
		SizeBytes:       &size,
		FingerprintAlgo: &algo,
		Fingerprint:     sum[:],
		PolicyIdentity:  e.Cfg.Policy.Identity,
	})
	if err != nil {
		t.Fatalf("register job through the real ledger: %v", err)
	}
	if job.State != jobs.StatePendingDispatch {
		t.Fatalf("a newly registered job is in state %q, expected %q", job.State, jobs.StatePendingDispatch)
	}
	if !jobs.IsJobID(job.JobID) {
		t.Fatalf("registered job has a non-canonical identity %q", job.JobID)
	}
	return job
}

// TestF3SchemaIsAppliedByTheApplication asserts the watcher applied the
// embedded migrations against the primary.
func TestF3SchemaIsAppliedByTheApplication(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseBaseline, PhasePrimaryRestarted, PhasePrimaryRecovered)
	led := e.Ledger(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	version, err := led.SchemaVersion(ctx)
	if err != nil {
		t.Fatalf("read schema version: %v", err)
	}
	if version < 1 {
		t.Fatalf("no migration is applied (version=%d); the watcher did not initialise the schema", version)
	}
	t.Logf("schema version on the primary: %d", version)
}

// TestF3WriteAndReadThroughTheRealLedger exercises an actual durable write,
// the append-only history, and a read back.
func TestF3WriteAndReadThroughTheRealLedger(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseBaseline)
	led := e.Ledger(t)

	job := registerSyntheticJob(t, led, e, "f3-rw")
	e.SaveState(t, "f3-rw-job", job.JobID)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	read, err := led.GetJob(ctx, job.JobID)
	if err != nil {
		t.Fatalf("read the job back from the primary: %v", err)
	}
	if read.SourceName != job.SourceName || read.PolicyVersion != job.PolicyVersion {
		t.Fatalf("the row read back does not match what was written")
	}
	if read.SizeBytes == nil || *read.SizeBytes != *job.SizeBytes {
		t.Fatalf("size was not persisted")
	}
	if len(read.Fingerprint) != sha256.Size {
		t.Fatalf("content fingerprint was not persisted (%d bytes)", len(read.Fingerprint))
	}

	events, err := led.Events(ctx, job.JobID)
	if err != nil {
		t.Fatalf("read job history: %v", err)
	}
	if len(events) == 0 || events[0].EventType != jobs.EventRegistered {
		t.Fatalf("the append-only history does not begin with a registration event: %+v", events)
	}

	// A missing job must be reported as missing, never as an empty success.
	if _, err := led.GetJob(ctx, "00000000-0000-0000-0000-000000000000"); !errors.Is(err, ledger.ErrNotFound) {
		t.Fatalf("an absent job returned %v, expected ErrNotFound", err)
	}
}

// TestF3ReplicationReachesTheStandby writes on the primary and reads the row
// back from the standby through the application's replica pool.
func TestF3ReplicationReachesTheStandby(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseBaseline, PhaseReplicaRecovered, PhasePrimaryRestarted)
	led := e.Ledger(t)

	if !led.HasReplica() {
		t.Fatalf("no standby endpoint is configured; F3 requires a real replicated cluster and will not accept a single instance")
	}

	job := registerSyntheticJob(t, led, e, "f3-replication-"+string(e.Phase))

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Asynchronous replication means the read is eventually consistent. The
	// window is bounded and a failure to converge is a real failure.
	start := time.Now()
	err := waitForErr(45*time.Second, func() error {
		_, gerr := led.GetJobFromReplica(ctx, job.JobID)
		return gerr
	})
	if err != nil {
		t.Fatalf("the row never reached the standby within 45s: %v", err)
	}
	lag := time.Since(start)
	t.Logf("row %s became visible on the standby after %s", job.JobID, lag.Round(time.Millisecond))

	inRecovery, err := led.PingReplica(ctx)
	if err != nil {
		t.Fatalf("probe the standby: %v", err)
	}
	if !inRecovery {
		t.Fatalf("the standby is not in recovery; it has been promoted and is no longer replicating")
	}

	status, err := led.ReplicationStatus(ctx)
	if err != nil {
		t.Fatalf("read pg_stat_replication on the primary: %v", err)
	}
	if len(status) == 0 {
		t.Fatalf("pg_stat_replication is empty: no standby is streaming from the primary")
	}
	var streaming int
	var report strings.Builder
	fmt.Fprintf(&report, "phase=%s replica_visibility_lag=%s\n", e.Phase, lag.Round(time.Millisecond))
	for _, s := range status {
		fmt.Fprintf(&report, "application_name=%s state=%s sync_state=%s sent_lsn=%s replay_lsn=%s replay_lag_bytes=%d\n",
			s.ApplicationName, s.State, s.SyncState, s.SentLSN, s.ReplayLSN, s.ReplayLagBytes)
		if s.State == "streaming" {
			streaming++
		}
		// The topology is deliberately asynchronous; asserting it keeps the
		// documented recovery guarantee honest.
		if s.SyncState != "async" {
			t.Errorf("standby %q reports sync_state %q; the documented topology is asynchronous", s.ApplicationName, s.SyncState)
		}
	}
	if streaming == 0 {
		t.Fatalf("no walsender is in the streaming state:\n%s", report.String())
	}
	e.WriteEvidence(t, "f3-replication-status.txt", []byte(report.String()))
}

// TestF3StandbyRejectsWrites asserts the standby is a read-only hot standby,
// so a misrouted write cannot silently diverge the cluster.
func TestF3StandbyRejectsWrites(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseBaseline)
	led := e.Ledger(t)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	err := led.ExecOnReplica(ctx,
		`INSERT INTO schema_migrations (version, name) VALUES (-1, 'integration-must-fail')`)
	if err == nil {
		t.Fatalf("the standby accepted a write; it is not a read-only hot standby")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "read-only") {
		t.Errorf("the standby rejected the write with an unexpected error: %v", err)
	}
	t.Logf("standby correctly rejected a write: %v", err)
}

// TestF3PrimaryEndpointIsNotAStandby asserts the application refuses to treat
// a standby answering on the primary endpoint as a healthy primary.
func TestF3PrimaryEndpointIsNotAStandby(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseBaseline, PhasePrimaryRestarted, PhasePrimaryRecovered)
	led := e.Ledger(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := led.PingPrimary(ctx); err != nil {
		t.Fatalf("primary probe failed: %v", err)
	}
}

// TestF3DataPersistsAcrossPrimaryRestart runs after the orchestrator has
// restarted the primary container. It reads back a row written before the
// restart, which is what makes this a persistence assertion rather than a
// connectivity one.
func TestF3DataPersistsAcrossPrimaryRestart(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhasePrimaryRestarted)
	led := e.Ledger(t)

	jobID := e.LoadState(t, "f3-rw-job")

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var job ledger.Job
	err := waitForErr(45*time.Second, func() error {
		var gerr error
		job, gerr = led.GetJob(ctx, jobID)
		return gerr
	})
	if err != nil {
		t.Fatalf("a row written before the primary restart is not readable afterwards: %v", err)
	}
	if job.JobID != jobID {
		t.Fatalf("read back the wrong row")
	}
	if len(job.Fingerprint) != sha256.Size {
		t.Fatalf("the persisted fingerprint did not survive the restart")
	}

	events, err := led.Events(ctx, jobID)
	if err != nil {
		t.Fatalf("read retained history after the restart: %v", err)
	}
	if len(events) == 0 {
		t.Fatalf("the retained history for %s is empty after the restart", jobID)
	}
	t.Logf("job %s survived the primary restart with %d retained history rows", jobID, len(events))

	e.WriteEvidence(t, "f3-persistence.txt",
		[]byte(fmt.Sprintf("job_id=%s state=%s history_rows=%d\n", job.JobID, job.State, len(events))))
}

// TestF3ApplicationsRecoverAfterPrimaryReturns asserts readiness comes back on
// its own once the primary is available again, without a container restart.
func TestF3ApplicationsRecoverAfterPrimaryReturns(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhasePrimaryRecovered, PhasePrimaryRestarted)

	for _, url := range append([]string{e.WatcherURL}, e.RenamerURLs...) {
		u := url
		err := waitForErr(90*time.Second, func() error {
			r, _, body, gerr := GetReadiness(u)
			if gerr != nil {
				return gerr
			}
			if ok, category, _ := r.Check(telemetry.DepPostgresPrimary); !ok {
				return fmt.Errorf("%s still reports postgres_primary not ok (category=%q, body=%s)", u, category, body)
			}
			if !r.Ready {
				return fmt.Errorf("%s still reports not ready: %s", u, body)
			}
			return nil
		})
		if err != nil {
			t.Errorf("%v", err)
		}
	}
}

// TestF3AccountingReflectsTheLedger asserts the exposed job counts come from
// committed rows, and that nothing claims a delivery.
func TestF3AccountingReflectsTheLedger(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseBaseline)
	led := e.Ledger(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	snap, err := led.Snapshot(ctx)
	if err != nil {
		t.Fatalf("ledger snapshot: %v", err)
	}

	var body string
	err = waitForErr(30*time.Second, func() error {
		var gerr error
		body, gerr = GetMetrics(e.WatcherURL)
		if gerr != nil {
			return gerr
		}
		runs, merr := requireMetricErr(body, "fn_accounting_runs_total", map[string]string{"outcome": "ok"})
		if merr != nil {
			return merr
		}
		if runs < 1 {
			return fmt.Errorf("the accounting worker has not completed a pass yet")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("%v", err)
	}

	// Every state in the closed set must be exposed, including the ones no
	// code path reaches yet, so a scraper can tell zero from absent.
	for _, state := range jobs.States() {
		_ = metricValue(t, body, "fn_jobs", map[string]string{"state": string(state)})
	}

	// This increment records holds; it never records a delivery or an
	// uncertain outcome. A non-zero count here would mean the foundation is
	// claiming an outcome it cannot produce.
	for _, state := range []jobs.State{jobs.StateDelivered, jobs.StateUncertain} {
		if n := snap.ByState[state]; n != 0 {
			t.Errorf("the ledger holds %d jobs in state %q, but no code path in this increment produces that state", n, state)
		}
		if v := metricValue(t, body, "fn_jobs", map[string]string{"state": string(state)}); v != 0 {
			t.Errorf("fn_jobs{state=%q} is %v; this increment must never report that outcome", state, v)
		}
	}

	e.WriteEvidence(t, "f3-accounting.txt", []byte(describeSamples(body, "fn_jobs")))
}
