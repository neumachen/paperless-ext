//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/broker"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/jobs"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/storage"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/telemetry"
)

// F5 — dependency failure and recovery.
//
// The interruptions are real and isolated: the orchestrator stops individual
// containers in this project only. What is asserted is that readiness tells
// the truth, that errors are visible, that behaviour stays bounded, and that
// recovery happens without restarting the applications.

// TestF5ReadinessIsFalseWhenThePrimaryIsDown asserts the applications do not
// claim readiness while their required durable store is unreachable.
func TestF5ReadinessIsFalseWhenThePrimaryIsDown(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhasePrimaryDown)

	if !e.ExpectedDown(telemetry.DepPostgresPrimary) {
		t.Fatalf("phase %q must declare postgres_primary as interrupted", e.Phase)
	}

	for _, url := range append([]string{e.WatcherURL}, e.RenamerURLs...) {
		u := url
		err := waitForErr(60*time.Second, func() error {
			r, status, body, gerr := GetReadiness(u)
			if gerr != nil {
				return gerr
			}
			if r.Ready {
				return fmt.Errorf("%s still claims readiness with the primary down: %s", u, body)
			}
			if status != http.StatusServiceUnavailable {
				return fmt.Errorf("%s returned status %d while not ready, expected 503", u, status)
			}
			ok, category, found := r.Check(telemetry.DepPostgresPrimary)
			if !found {
				return fmt.Errorf("%s does not report a postgres_primary check", u)
			}
			if ok {
				return fmt.Errorf("%s reports postgres_primary ok while it is down", u)
			}
			if category == "" {
				return fmt.Errorf("%s reports no failure category for postgres_primary", u)
			}
			// The category must be a sanitized classification, never a raw
			// driver error carrying a DSN.
			if !jobs.SafeIdentifier(category) {
				return fmt.Errorf("%s reported an unsafe readiness category %q", u, category)
			}
			return nil
		})
		if err != nil {
			t.Errorf("%v", err)
			continue
		}
		t.Logf("%s correctly reports itself not ready with the primary down", u)
	}
}

// TestF5MetricsRemainScrapableDuringOutage asserts telemetry keeps working
// while a dependency is down, and reports the dependency as down.
func TestF5MetricsRemainScrapableDuringOutage(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhasePrimaryDown, PhaseRabbitDown)

	var dep string
	switch e.Phase {
	case PhasePrimaryDown:
		dep = telemetry.DepPostgresPrimary
	case PhaseRabbitDown:
		dep = telemetry.DepRabbitMQ
	}

	for _, url := range append([]string{e.WatcherURL}, e.RenamerURLs...) {
		u := url
		err := waitForErr(60*time.Second, func() error {
			body, gerr := GetMetrics(u)
			if gerr != nil {
				return fmt.Errorf("%s /metrics is not scrapable during the outage: %w", u, gerr)
			}
			// A missing series must fail rather than be read as zero:
			// otherwise "the dependency reports down" would also be satisfied
			// by the metric never having been exposed.
			v, err := requireMetricErr(body, "fn_dependency_up", map[string]string{"dependency": dep})
			if err != nil {
				return fmt.Errorf("%s: %w", u, err)
			}
			if v != 0 {
				return fmt.Errorf("%s reports fn_dependency_up{dependency=%q}=%v during the outage", u, dep, v)
			}
			ready, err := requireMetricErr(body, "fn_ready", nil)
			if err != nil {
				return fmt.Errorf("%s: %w", u, err)
			}
			if ready != 0 {
				return fmt.Errorf("%s reports fn_ready=%v during the outage", u, ready)
			}
			return nil
		})
		if err != nil {
			t.Errorf("%v", err)
		}
	}
}

// TestF5ErrorsAreVisibleAndSanitized asserts the outage produced visible log
// output, classified rather than raw.
func TestF5ErrorsAreVisibleAndSanitized(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhasePrimaryDown, PhaseRabbitDown)

	// Scoped to this fault's own interval. The collected log holds the whole
	// run, so scanning all of it would let a warning produced by an earlier
	// phase satisfy this phase's claim.
	label := "primary_down"
	if e.Phase == PhaseRabbitDown {
		label = "rabbit_down"
	}
	since := e.Since(t, label)

	service := "watcher"
	records := recordsSince(t, e, service, since)

	var errorEvents, classified int
	for _, r := range records {
		if r.String("level") != "ERROR" && r.String("level") != "WARN" {
			continue
		}
		errorEvents++
		if kind := r.String("error_kind"); kind != "" {
			classified++
			if !jobs.SafeIdentifier(kind) {
				t.Errorf("log record carries an unsafe error_kind %q: %s", kind, r.Raw)
			}
		}
	}
	if errorEvents == 0 {
		t.Errorf("%s logged no warning or error within the %q fault interval (from %s); the failure was not visible",
			service, e.Phase, since.Format(time.RFC3339))
	}
	if classified == 0 {
		t.Errorf("%s logged failures without a classified error_kind", service)
	}
	t.Logf("%s logged %d warning/error records within the fault interval, %d carrying a classified error_kind",
		service, errorEvents, classified)
	e.WriteEvidence(t, "f5-fault-interval-errors.txt", []byte(fmt.Sprintf(
		"phase=%s fault_interval_start=%s records_in_interval=%d warnings_or_errors=%d classified=%d\n",
		e.Phase, since.Format(time.RFC3339Nano), len(records), errorEvents, classified)))
}

// TestF5NoOutcomeIsInventedDuringAnOutage asserts an outage never turns into a
// claimed success: no job may reach delivered or uncertain, and no job may be
// held while the durable store is unreachable.
func TestF5NoOutcomeIsInventedDuringAnOutage(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhasePrimaryRecovered)
	led := e.Ledger(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	snap, err := led.Snapshot(ctx)
	if err != nil {
		t.Fatalf("ledger snapshot after recovery: %v", err)
	}
	if n := snap.ByState[jobs.StateDelivered]; n != 0 {
		t.Errorf("%d jobs are recorded as delivered; the outage produced an outcome no code path can produce", n)
	}
	if n := snap.ByState[jobs.StateUncertain]; n != 0 {
		t.Errorf("%d jobs are recorded as uncertain; no code path in this increment writes that state", n)
	}
	t.Logf("ledger after the outage: %v", snap.ByState)
}

// TestF5DeliveryDuringALedgerOutageIsBoundedAndNotLost is the assertion that
// the renamer's behaviour stays bounded when it receives work it cannot
// durably settle.
//
// Setup: an already-held job from the baseline phase is republished to the real
// work queue while the primary is down. A renamer receives the delivery, finds
// it cannot record ownership, returns the delivery and stops consuming for a
// backoff rather than accepting the same message again immediately. The
// message must stay in the queue and the delivery count must stay small.
//
// This matters because of the measured broker behaviour recorded in F4: an
// explicit requeue does not advance the quorum queue's delivery counter, so
// x-delivery-limit cannot bound this loop. Without the detach, the instance
// would spin thousands of times per second through the outage.
func TestF5DeliveryDuringALedgerOutageIsBoundedAndNotLost(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhasePrimaryDown)

	jobID := e.LoadState(t, "f4-e2e-job")

	conn, _ := e.Broker(t)
	pub := broker.NewPublisher(conn, broker.TopologyFromConfig(e.Cfg.Broker), e.Log, e.Cfg.Broker.ConfirmTimeout)
	t.Cleanup(pub.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	// A duplicate message for an existing job. Duplicate delivery is allowed
	// by the contract and must be safe; here it is also the only way to hand a
	// renamer real work while its durable store is unreachable.
	result, err := pub.Publish(ctx, jobs.Message{
		ContractVersion: jobs.ContractVersion,
		JobID:           jobID,
		Attempt:         99,
		EnqueuedAt:      time.Now().UTC(),
	})
	if result != broker.PublishConfirmed {
		t.Fatalf("could not publish the duplicate into the real work queue: %s (%v)", result, err)
	}
	e.SaveState(t, "f5-ledger-outage-job", jobID)
	t.Logf("published a duplicate for job %s while the primary is down", jobID)

	// Give the renamers time to take the delivery, fail, and detach.
	time.Sleep(25 * time.Second)

	var (
		totalRequeued float64
		anyConsumerUp bool
		report        strings.Builder
	)
	for _, url := range e.RenamerURLs {
		body, merr := GetMetrics(url)
		if merr != nil {
			t.Fatalf("%s /metrics: %v", url, merr)
		}
		requeued := requireMetric(t, body, "fn_deliveries_total", map[string]string{"outcome": "requeued"})
		up := requireMetric(t, body, "fn_consumer_up", nil)
		inflight := requireMetric(t, body, "fn_deliveries_in_flight", nil)
		held := requireMetric(t, body, "fn_deliveries_total", map[string]string{"outcome": "held"})
		totalRequeued += requeued
		if up == 1 {
			anyConsumerUp = true
		}
		fmt.Fprintf(&report, "%s requeued=%v held=%v consumer_up=%v in_flight=%v\n", url, requeued, held, up, inflight)

		// Nothing may be recorded as held while the ledger is unreachable
		// beyond what the baseline phase already produced.
		if inflight > float64(e.Cfg.Concurrency) {
			t.Errorf("%s reports %v deliveries in flight, above its bound of %d", url, inflight, e.Cfg.Concurrency)
		}
	}
	t.Logf("renamer state during the ledger outage:\n%s", report.String())

	if totalRequeued < 1 {
		t.Fatalf("no renamer returned the delivery; the duplicate was not picked up, so this phase proves nothing:\n%s", report.String())
	}
	// The bounded-behaviour assertion. A hot requeue loop would reach
	// thousands within 25 seconds; the detach keeps it to a handful.
	const hotLoopThreshold = 60
	if totalRequeued > hotLoopThreshold {
		t.Errorf("the renamers returned the delivery %v times in ~25s; that is a hot requeue loop, not bounded behaviour:\n%s",
			totalRequeued, report.String())
	}
	if anyConsumerUp {
		t.Errorf("a renamer still holds an active consumer while it cannot settle deliveries:\n%s", report.String())
	}

	// The message must still be queued: it was neither settled nor lost.
	depth, _, derr := pub.QueueDepth(e.Cfg.Broker.Queue)
	if derr != nil {
		t.Fatalf("read the work queue depth: %v", derr)
	}
	if depth < 1 {
		t.Errorf("the work queue is empty; the delivery was settled or lost during the outage")
	}
	fmt.Fprintf(&report, "work_queue_depth=%d\n", depth)

	e.WriteEvidence(t, "f5-ledger-outage-bounded.txt", []byte(fmt.Sprintf(
		"job_id=%s\nrequeue_decisions_total=%v hot_loop_threshold=%d\nany_consumer_attached=%t\n%s",
		jobID, totalRequeued, hotLoopThreshold, anyConsumerUp, report.String())))
}

// TestF5LedgerOutageDeliveryIsSettledAfterRecovery closes the loop: once the
// primary returns, the renamer reattaches on its own and settles the duplicate
// without changing the job's durable outcome.
func TestF5LedgerOutageDeliveryIsSettledAfterRecovery(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhasePrimaryRecovered)
	led := e.Ledger(t)

	jobID := e.LoadState(t, "f5-ledger-outage-job")

	conn, _ := e.Broker(t)
	pub := broker.NewPublisher(conn, broker.TopologyFromConfig(e.Cfg.Broker), e.Log, e.Cfg.Broker.ConfirmTimeout)
	t.Cleanup(pub.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	before, err := led.GetJob(ctx, jobID)
	if err != nil {
		t.Fatalf("read the job after recovery: %v", err)
	}

	if derr := waitForErr(120*time.Second, func() error {
		n, _, qerr := pub.QueueDepth(e.Cfg.Broker.Queue)
		if qerr != nil {
			return qerr
		}
		if n != 0 {
			return fmt.Errorf("the work queue still holds %d message(s)", n)
		}
		return nil
	}); derr != nil {
		t.Fatalf("the delivery held through the outage was not settled after recovery: %v", derr)
	}

	after, err := led.GetJob(ctx, jobID)
	if err != nil {
		t.Fatalf("re-read the job: %v", err)
	}

	// A duplicate delivery must not change the outcome or invent a new one.
	if after.State != jobs.StateHeld {
		t.Errorf("the job moved to state %q after a duplicate delivery, expected it to stay %q", after.State, jobs.StateHeld)
	}
	if after.DeliveryAttempts < before.DeliveryAttempts {
		t.Errorf("the recorded delivery attempts went backwards: %d then %d", before.DeliveryAttempts, after.DeliveryAttempts)
	}
	if after.NormalizedName != nil || after.ReservedName != nil {
		t.Errorf("a duplicate delivery produced a normalized or reserved name")
	}

	// Consumers must be attached again without any container restart.
	for _, url := range e.RenamerURLs {
		u := url
		if werr := waitForErr(60*time.Second, func() error {
			body, merr := GetMetrics(u)
			if merr != nil {
				return merr
			}
			v, merr2 := requireMetricErr(body, "fn_consumer_up", nil)
			if merr2 != nil {
				return fmt.Errorf("%s: %w", u, merr2)
			}
			if v != 1 {
				return fmt.Errorf("%s reports fn_consumer_up=%v", u, v)
			}
			return nil
		}); werr != nil {
			t.Errorf("a renamer did not reattach after the outage: %v", werr)
		}
	}

	t.Logf("duplicate delivery settled after recovery: state=%s delivery_attempts=%d->%d",
		after.State, before.DeliveryAttempts, after.DeliveryAttempts)
	e.WriteEvidence(t, "f5-ledger-outage-recovery.txt", []byte(fmt.Sprintf(
		"job_id=%s state_before=%s state_after=%s delivery_attempts=%d->%d work_queue_drained=true\n",
		jobID, before.State, after.State, before.DeliveryAttempts, after.DeliveryAttempts)))
}

// TestF5PendingWorkIsNotStrandedByABrokerOutage asserts a job registered while
// the broker is down stays discoverable and is dispatched once it returns.
func TestF5PendingWorkIsNotStrandedByABrokerOutage(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseRabbitDown)
	led := e.Ledger(t)

	job := registerSyntheticJob(t, led, e, "f5-broker-outage")
	e.SaveState(t, "f5-outage-job", job.JobID)

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	// Give the dispatcher time to run a pass. It must not consume the job: with
	// no broker there is nowhere to publish it, so the job must remain
	// discoverable rather than being marked dispatched.
	time.Sleep(10 * time.Second)

	cur, err := led.GetJob(ctx, job.JobID)
	if err != nil {
		t.Fatalf("read the job during the broker outage: %v", err)
	}
	if cur.State == jobs.StateDispatched {
		t.Fatalf("job %s is marked dispatched while the broker is down; a publication was recorded that cannot have happened", job.JobID)
	}
	if cur.DispatchedAt != nil {
		t.Fatalf("job %s has a dispatch timestamp while the broker is down", job.JobID)
	}
	t.Logf("job %s remains in state %q during the broker outage", cur.JobID, cur.State)
}

// TestF5PendingWorkIsDispatchedAfterRecovery closes the loop on the previous
// test once the broker is back.
func TestF5PendingWorkIsDispatchedAfterRecovery(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseRabbitRecovered)
	led := e.Ledger(t)

	jobID := e.LoadState(t, "f5-outage-job")

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()

	err := waitForErr(120*time.Second, func() error {
		cur, gerr := led.GetJob(ctx, jobID)
		if gerr != nil {
			return gerr
		}
		if cur.State != jobs.StateHeld {
			return fmt.Errorf("job %s is in state %q, waiting for it to be dispatched and settled", jobID, cur.State)
		}
		if cur.DispatchedAt == nil {
			return fmt.Errorf("job %s was settled without a recorded dispatch", jobID)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("work registered during the broker outage was not recovered: %v", err)
	}

	cur, _ := led.GetJob(ctx, jobID)
	t.Logf("job %s recovered after the broker returned: state=%s dispatch_attempts=%d",
		cur.JobID, cur.State, cur.DispatchAttempts)
	e.WriteEvidence(t, "f5-broker-recovery.txt", []byte(fmt.Sprintf(
		"job_id=%s state=%s dispatch_attempts=%d delivery_attempts=%d\n",
		cur.JobID, cur.State, cur.DispatchAttempts, cur.DeliveryAttempts)))
}

// TestF5BrokerReconnectsWithoutRestart asserts the applications reattach to
// the broker on their own, and that the reconnection is counted.
func TestF5BrokerReconnectsWithoutRestart(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseRabbitRecovered, PhaseRabbitRestarted)

	for _, url := range append([]string{e.WatcherURL}, e.RenamerURLs...) {
		u := url
		err := waitForErr(120*time.Second, func() error {
			r, _, body, gerr := GetReadiness(u)
			if gerr != nil {
				return gerr
			}
			if ok, category, _ := r.Check(telemetry.DepRabbitMQ); !ok {
				return fmt.Errorf("%s still reports rabbitmq not ok (category=%q)", u, category)
			}
			if !r.Ready {
				return fmt.Errorf("%s still not ready: %s", u, body)
			}
			return nil
		})
		if err != nil {
			t.Errorf("%v", err)
			continue
		}
		body, merr := GetMetrics(u)
		if merr != nil {
			t.Errorf("%s /metrics: %v", u, merr)
			continue
		}
		if v := requireMetric(t, body, "fn_broker_reconnects_total", nil); v < 1 {
			t.Errorf("%s reports fn_broker_reconnects_total=%v; the reconnection was not counted", u, v)
		}
	}
}

// TestF5StandbyLossIsReportedTruthfully asserts a lost standby is reported as
// such, and that its effect on readiness matches the configured requirement.
func TestF5StandbyLossIsReportedTruthfully(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseReplicaDown)

	required := e.Cfg.Database.ReplicaRequired
	for _, url := range append([]string{e.WatcherURL}, e.RenamerURLs...) {
		u := url
		err := waitForErr(60*time.Second, func() error {
			r, _, body, gerr := GetReadiness(u)
			if gerr != nil {
				return gerr
			}
			ok, category, found := r.Check(telemetry.DepPostgresReplica)
			if !found {
				return fmt.Errorf("%s does not report a postgres_replica check", u)
			}
			if ok {
				return fmt.Errorf("%s reports the standby ok while it is stopped", u)
			}
			if category == "" {
				return fmt.Errorf("%s reports no category for the failed standby", u)
			}
			if required && r.Ready {
				return fmt.Errorf("%s claims readiness with a required standby down: %s", u, body)
			}
			if !required && !r.Ready {
				return fmt.Errorf("%s is not ready although the standby is configured as optional: %s", u, body)
			}
			return nil
		})
		if err != nil {
			t.Errorf("%v", err)
			continue
		}
		t.Logf("%s reports the standby as down; replica_required=%t", u, required)
	}
}

// TestF5StorageFaultsAreDistinguished asserts the storage probe reports
// unavailable storage distinctly from an empty directory.
//
// This is the requirement that inaccessible or incorrectly mounted storage
// must never be read as file absence. The fixtures are constructed here in an
// isolated temporary tree owned by this test.
func TestF5StorageFaultsAreDistinguished(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseStorageFault, PhaseBaseline)

	base := t.TempDir()

	emptyDir := filepath.Join(base, "empty")
	if err := os.MkdirAll(emptyDir, 0o770); err != nil {
		t.Fatalf("create the empty-directory fixture: %v", err)
	}

	populated := filepath.Join(base, "populated")
	if err := os.MkdirAll(populated, 0o770); err != nil {
		t.Fatalf("create the populated fixture: %v", err)
	}
	if err := os.WriteFile(filepath.Join(populated, "synthetic.pdf"), []byte("synthetic"), 0o640); err != nil {
		t.Fatalf("write the populated fixture: %v", err)
	}

	missing := filepath.Join(base, "never-mounted")

	notDir := filepath.Join(base, "a-file-not-a-mount")
	if err := os.WriteFile(notDir, []byte("this is a file where a mount was expected"), 0o640); err != nil {
		t.Fatalf("write the not-a-directory fixture: %v", err)
	}

	denied := filepath.Join(base, "denied")
	if err := os.MkdirAll(denied, 0o000); err != nil {
		t.Fatalf("create the permission-denied fixture: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(denied, 0o700) })

	readOnly := filepath.Join(base, "read-only")
	if err := os.MkdirAll(readOnly, 0o500); err != nil {
		t.Fatalf("create the read-only fixture: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(readOnly, 0o700) })

	report := storage.Probe([]storage.Root{
		{Role: "empty", Path: emptyDir, WriteRequired: true},
		{Role: "populated", Path: populated, WriteRequired: true},
		{Role: "missing", Path: missing, WriteRequired: true},
		{Role: "notdir", Path: notDir, WriteRequired: true},
		{Role: "denied", Path: denied, WriteRequired: true},
		{Role: "readonly", Path: readOnly, WriteRequired: true},
	})

	want := map[string]storage.Status{
		"empty":     storage.StatusEmpty,
		"populated": storage.StatusOK,
		"missing":   storage.StatusMissing,
		"notdir":    storage.StatusNotADirectory,
		"denied":    storage.StatusPermissionDenied,
		"readonly":  storage.StatusNotWritable,
	}

	running := os.Geteuid()
	got := map[string]storage.Status{}
	for _, res := range report.Results {
		got[res.Role] = res.Status
	}

	for role, expect := range want {
		actual, ok := got[role]
		if !ok {
			t.Errorf("the probe did not report role %q", role)
			continue
		}
		if actual == expect {
			continue
		}
		// A process running as root bypasses directory permission bits, so the
		// two permission fixtures cannot be constructed for it. That is stated
		// rather than silently passed.
		if running == 0 && (role == "denied" || role == "readonly") {
			t.Logf("role %q reported %q instead of %q: this process runs as uid 0, "+
				"which bypasses permission bits, so the fixture cannot be constructed here",
				role, actual, expect)
			continue
		}
		t.Errorf("role %q reported status %q, expected %q", role, actual, expect)
	}

	// The core distinction: an absent root and an empty root must never share
	// a status, and only one of them is a healthy state.
	if got["missing"] == got["empty"] {
		t.Fatalf("an unmounted root and an empty directory both report %q; unavailable storage is being read as file absence", got["missing"])
	}
	if got["missing"].Available() {
		t.Fatalf("an unmounted root reports as available")
	}
	if !got["empty"].Available() {
		t.Fatalf("an empty but healthy directory reports as unavailable")
	}
	if report.Available() {
		t.Fatalf("a report containing unavailable roles claims overall availability")
	}
	if len(report.Unavailable()) == 0 {
		t.Fatalf("the report does not list any unavailable role")
	}

	payload, _ := json.MarshalIndent(got, "", "  ")
	e.WriteEvidence(t, "f5-storage-probe.json", payload)
}

// TestF5StorageFaultMakesTheApplicationNotReady asserts a misconfigured
// storage root produces a truthful not-ready application rather than a
// process that quietly treats the root as empty.
//
// The orchestrator starts a dedicated watcher container whose incoming root
// points at a path that was never mounted. Its database and broker are the
// healthy ones, so storage is the only thing that can be at fault.
func TestF5StorageFaultMakesTheApplicationNotReady(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseStorageFault)

	url := getenv("FN_TEST_FAULT_URL", "http://watcher-storage-fault:8080")

	var (
		r      Readiness
		status int
		raw    []byte
	)
	err := waitForErr(90*time.Second, func() error {
		var gerr error
		r, status, raw, gerr = GetReadiness(url)
		if gerr != nil {
			return fmt.Errorf("%s /readyz: %w", url, gerr)
		}
		if _, _, found := r.Check(telemetry.DepStorage); !found {
			return fmt.Errorf("%s has not reported a storage check yet", url)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("%v", err)
	}

	// Liveness must still answer: the process is running and is reporting a
	// fault, which is different from having crashed.
	if _, hstatus, herr := GetHealth(url); herr != nil || hstatus != http.StatusOK {
		t.Errorf("%s /healthz: status=%d err=%v; a storage fault should not kill the process", url, hstatus, herr)
	}

	if r.Ready {
		t.Fatalf("an application with an unmounted storage root claimed readiness: %s", raw)
	}
	if status != http.StatusServiceUnavailable {
		t.Errorf("%s returned status %d while not ready, expected 503", url, status)
	}

	ok, category, _ := r.Check(telemetry.DepStorage)
	if ok {
		t.Fatalf("the storage check passed with an unmounted root: %s", raw)
	}
	if category == "" {
		t.Fatalf("the storage check reports no category: %s", raw)
	}
	// The category names the role and the probe status, so an operator can see
	// that the root is missing rather than empty.
	if !strings.Contains(category, "missing") {
		t.Errorf("the storage category is %q; an unmounted root must be reported as missing, not as an empty directory", category)
	}
	if strings.Contains(category, "empty") {
		t.Fatalf("an unmounted root was reported as empty: %q", category)
	}
	t.Logf("the storage-fault container reports storage category %q", category)

	// The fault must also be visible to a scraper, distinctly from "empty".
	body, merr := GetMetrics(url)
	if merr != nil {
		t.Fatalf("%s /metrics: %v", url, merr)
	}
	if v := requireMetric(t, body, "fn_storage_root_available", map[string]string{"role": "incoming"}); v != 0 {
		t.Errorf("fn_storage_root_available{role=\"incoming\"} is %v with an unmounted root", v)
	}
	if v := requireMetric(t, body, "fn_storage_root_status", map[string]string{"role": "incoming", "status": "missing"}); v != 1 {
		t.Errorf("fn_storage_root_status{role=\"incoming\",status=\"missing\"} is %v, expected 1", v)
	}
	if v := requireMetric(t, body, "fn_storage_root_status", map[string]string{"role": "incoming", "status": "empty"}); v != 0 {
		t.Errorf("fn_storage_root_status{role=\"incoming\",status=\"empty\"} is %v; unavailable storage is being reported as empty", v)
	}
	if v := requireMetric(t, body, "fn_dependency_up", map[string]string{"dependency": telemetry.DepStorage}); v != 0 {
		t.Errorf("fn_dependency_up{dependency=\"storage\"} is %v with an unmounted root", v)
	}
	// The other dependencies are healthy, so storage must be the only fault.
	for _, dep := range []string{telemetry.DepPostgresPrimary, telemetry.DepRabbitMQ} {
		if v := requireMetric(t, body, "fn_dependency_up", map[string]string{"dependency": dep}); v != 1 {
			t.Errorf("fn_dependency_up{dependency=%q} is %v; the fault is not isolated to storage", dep, v)
		}
	}

	e.WriteEvidence(t, "f5-storage-fault-readyz.json", raw)
	e.WriteEvidence(t, "f5-storage-fault-metrics.txt",
		[]byte(describeSamples(body, "fn_storage_root_status")+describeSamples(body, "fn_dependency_up")))
}
