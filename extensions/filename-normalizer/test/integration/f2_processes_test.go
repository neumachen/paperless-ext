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

	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/jobs"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/telemetry"
)

// F2 — actual application processes.
//
// One watcher and two independent renamer containers run as their own
// processes. These assertions cover configuration, readiness and — in the
// drained phase — graceful termination. They deliberately stop short of
// claiming concurrent document processing: two containers establish deployment
// capability, and no overlapping document processing exists yet because
// normalization is unimplemented.

// TestF2AllApplicationsAreReady reads each application's real readiness.
func TestF2AllApplicationsAreReady(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseBaseline, PhasePrimaryRecovered, PhaseRabbitRecovered, PhaseReplicaRecovered, PhasePrimaryRestarted)

	if len(e.RenamerURLs) < 2 {
		t.Fatalf("F2 requires at least two independent renamer instances; got %d", len(e.RenamerURLs))
	}

	var report strings.Builder
	for _, url := range append([]string{e.WatcherURL}, e.RenamerURLs...) {
		r, status, body, err := GetReadiness(url)
		if err != nil {
			t.Fatalf("%s /readyz: %v", url, err)
		}
		fmt.Fprintf(&report, "%s status=%d body=%s\n", url, status, body)

		if !r.Ready || status != http.StatusOK {
			t.Errorf("%s is not ready in phase %q: status=%d body=%s", url, e.Phase, status, body)
			continue
		}
		for _, dep := range []string{telemetry.DepPostgresPrimary, telemetry.DepRabbitMQ, telemetry.DepStorage} {
			ok, category, found := r.Check(dep)
			if !found {
				t.Errorf("%s readiness does not report a %s check", url, dep)
				continue
			}
			if !ok {
				t.Errorf("%s reports %s not ok (category=%q)", url, dep, category)
			}
		}
	}
	e.WriteEvidence(t, "f2-readiness.txt", []byte(report.String()))
}

// TestF2InstancesAreDistinct asserts three separately configured application
// processes are answering, not one container reached through three addresses.
func TestF2InstancesAreDistinct(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseBaseline)

	type identity struct {
		Application string `json:"application"`
		Instance    string `json:"instance"`
		Version     string `json:"version"`
	}

	byInstance := map[string]string{}
	apps := map[string]int{}

	for _, url := range append([]string{e.WatcherURL}, e.RenamerURLs...) {
		body, status, err := GetHealth(url)
		if err != nil || status != 200 {
			t.Fatalf("%s /healthz: status=%d err=%v", url, status, err)
		}
		var id identity
		if err := json.Unmarshal(body, &id); err != nil {
			t.Fatalf("%s /healthz is not JSON: %v", url, err)
		}
		if id.Instance == "" {
			t.Fatalf("%s reports no instance identity", url)
		}
		if other, dup := byInstance[id.Instance]; dup {
			t.Errorf("%s and %s both report instance %q; they are not independent instances", url, other, id.Instance)
		}
		byInstance[id.Instance] = url
		apps[id.Application]++
	}

	if len(byInstance) != 3 {
		t.Errorf("expected 3 distinct instances, found %d: %v", len(byInstance), byInstance)
	}
	if apps["watcher"] != 1 {
		t.Errorf("expected exactly one watcher, found %d", apps["watcher"])
	}
	if apps["renamer"] != 2 {
		t.Errorf("expected exactly two renamers, found %d", apps["renamer"])
	}

	// Each instance keeps its own metric state, which one shared process could
	// not do.
	for instance, url := range byInstance {
		body, err := GetMetrics(url)
		if err != nil {
			t.Fatalf("%s /metrics: %v", url, err)
		}
		if len(samplesFor(body, "process_start_time_seconds")) != 1 {
			t.Errorf("%s (%s) does not expose its own process collector", url, instance)
		}
	}
	e.WriteEvidence(t, "f2-instances.txt", []byte(fmt.Sprintf("%v\n", byInstance)))
}

// TestF2BothRenamersTakeWorkFromTheSharedQueue asserts two independently
// deployed renamer containers really are both consuming one queue.
//
// This establishes deployment capability and shared-queue consumption. It is
// deliberately not a claim of concurrent document processing: no document is
// processed at all in this increment, and the assertion below only shows that
// work was distributed across both instances.
func TestF2BothRenamersTakeWorkFromTheSharedQueue(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseBaseline)
	led := e.Ledger(t)

	const batch = 24
	ids := make([]string, 0, batch)
	for i := 0; i < batch; i++ {
		job := registerSyntheticJob(t, led, e, fmt.Sprintf("f2-distribution-%02d", i))
		ids = append(ids, job.JobID)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	// Wait for the batch to reach a durable outcome through the applications.
	err := waitForErr(150*time.Second, func() error {
		pending := 0
		for _, id := range ids {
			job, gerr := led.GetJob(ctx, id)
			if gerr != nil {
				return gerr
			}
			if job.State != jobs.StateHeld {
				pending++
			}
		}
		if pending > 0 {
			return fmt.Errorf("%d of %d jobs have not reached a durable outcome yet", pending, batch)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("the batch did not drain through the renamers: %v", err)
	}

	// The actor on each delivery event is the instance that handled it.
	actors := map[string]int{}
	for _, id := range ids {
		events, eerr := led.Events(ctx, id)
		if eerr != nil {
			t.Fatalf("read history for %s: %v", id, eerr)
		}
		for _, ev := range events {
			if ev.EventType == jobs.EventHeld {
				actors[ev.Actor]++
			}
		}
	}

	renamerActors := map[string]int{}
	for actor, n := range actors {
		if strings.HasPrefix(actor, "renamer-") {
			renamerActors[actor] = n
		}
	}
	if len(renamerActors) < 2 {
		t.Errorf("only %d renamer instance(s) settled any of the %d jobs (%v); the queue is not being shared across both containers",
			len(renamerActors), batch, actors)
	}
	t.Logf("work distribution across renamer instances: %v", renamerActors)
	e.WriteEvidence(t, "f2-work-distribution.txt",
		[]byte(fmt.Sprintf("batch=%d distribution=%v\n", batch, renamerActors)))
}

// TestF2RenamerConcurrencyIsBoundedUnderLoad asserts each renamer honours its
// configured bound while it is actually busy.
//
// Sampling the in-flight gauge after a batch has drained would read zero and
// pass whatever the bound was, so the sampling happens during a batch and the
// test requires having observed the instance genuinely in flight.
func TestF2RenamerConcurrencyIsBoundedUnderLoad(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseBaseline)
	led := e.Ledger(t)

	// The bound is read from each instance, not from this observer's own
	// configuration. The suite does not run with the renamers' settings, so
	// comparing against its own default would assert the wrong number — and
	// would have compared an observed 1 against a bound of 1 while the
	// instances were actually configured for 2.
	bounds := map[string]int{}
	for _, url := range e.RenamerURLs {
		body, err := GetMetrics(url)
		if err != nil {
			t.Fatalf("%s /metrics: %v", url, err)
		}
		limit := requireMetric(t, body, "fn_renamer_concurrency_limit", nil)
		if limit < 1 {
			t.Fatalf("%s reports a non-positive concurrency limit (%v)", url, limit)
		}
		prefetch := requireMetric(t, body, "fn_renamer_prefetch_limit", nil)
		if prefetch < limit {
			t.Errorf("%s reports prefetch %v below its concurrency limit %v", url, prefetch, limit)
		}
		bounds[url] = int(limit)
		t.Logf("%s declares concurrency=%v prefetch=%v", url, limit, prefetch)
	}

	// Large enough that the batch is still draining while the sampling loop
	// runs, so the gauge is read under load rather than at rest.
	const batch = 250
	ids := make([]string, 0, batch)
	for i := 0; i < batch; i++ {
		job := registerSyntheticJob(t, led, e, fmt.Sprintf("f2-load-%03d", i))
		ids = append(ids, job.JobID)
	}

	type observed struct {
		maxInFlight  float64
		samples      int
		firstHandled float64
		lastHandled  float64
	}
	seen := map[string]*observed{}
	for _, url := range e.RenamerURLs {
		seen[url] = &observed{firstHandled: -1}
	}

	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		busy := false
		for _, url := range e.RenamerURLs {
			body, err := GetMetrics(url)
			if err != nil {
				t.Fatalf("%s /metrics: %v", url, err)
			}
			o := seen[url]
			o.samples++

			want := bounds[url]
			inflight := requireMetric(t, body, "fn_deliveries_in_flight", nil)
			if inflight > o.maxInFlight {
				o.maxInFlight = inflight
			}
			// The hard bound: exceeding it at any sample is a failure.
			if inflight > float64(want) {
				t.Fatalf("%s reports %v deliveries in flight, above its own declared bound of %d",
					url, inflight, want)
			}

			handled := requireMetric(t, body, "fn_delivery_duration_seconds_count", nil)
			if o.firstHandled < 0 {
				o.firstHandled = handled
			}
			o.lastHandled = handled

			if up := requireMetric(t, body, "fn_consumer_up", nil); up != 1 {
				t.Errorf("%s does not hold an active consumer while work is queued (fn_consumer_up=%v)", url, up)
			}
			if inflight > 0 {
				busy = true
			}
		}
		if busy {
			// Keep sampling a little longer once load has been seen, to give
			// the bound a chance to be violated if it can be.
			deadline = minTime(deadline, time.Now().Add(8*time.Second))
		}
		time.Sleep(50 * time.Millisecond)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if err := waitForErr(150*time.Second, func() error {
		pending := 0
		for _, id := range ids {
			j, gerr := led.GetJob(ctx, id)
			if gerr != nil {
				return gerr
			}
			if j.State != jobs.StateHeld {
				pending++
			}
		}
		if pending > 0 {
			return fmt.Errorf("%d of %d jobs have not reached a durable outcome", pending, batch)
		}
		return nil
	}); err != nil {
		t.Fatalf("the load batch did not drain: %v", err)
	}

	var report strings.Builder
	sawLoad := false
	progressed := false
	for _, url := range e.RenamerURLs {
		o := seen[url]
		delta := o.lastHandled - o.firstHandled
		fmt.Fprintf(&report, "%s samples=%d max_in_flight=%v declared_bound=%d deliveries_handled_during_sampling=%v\n",
			url, o.samples, o.maxInFlight, bounds[url], delta)
		if o.maxInFlight >= 1 {
			sawLoad = true
		}
		if delta > 0 {
			progressed = true
		}
	}
	// Without these two, the bound was never actually exercised.
	if !sawLoad {
		t.Errorf("no renamer was ever observed with a delivery in flight, so the bound was sampled at rest:\n%s", report.String())
	}
	if !progressed {
		t.Errorf("no renamer handled a delivery during the sampling window:\n%s", report.String())
	}

	fmt.Fprintf(&report, "batch=%d bound_source=fn_renamer_concurrency_limit bound_exceeded=false\n", batch)
	t.Logf("%s", report.String())
	e.WriteEvidence(t, "f2-concurrency-under-load.txt", []byte(report.String()))
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

// TestF2WatcherReportsDiscoveryAsUnimplemented asserts the watcher does not
// report a worker as running when it is not.
func TestF2WatcherReportsItsDiscoveryWorkerState(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseTelemetry)

	lines := readServiceLog(t, e, "watcher")
	found := false
	for _, ln := range lines {
		var rec map[string]any
		if json.Unmarshal([]byte(ln), &rec) != nil {
			continue
		}
		// Discovery is implemented now. The worker must say which of the two
		// states it is in -- running, or switched off by configuration -- so
		// an operator can tell "no work arrived" from "nothing is looking".
		switch rec["event"] {
		case "discovery_started":
			found = true
			for _, key := range []string{"completion_contract", "interval_seconds", "stability_seconds"} {
				if _, ok := rec[key]; !ok {
					t.Errorf("discovery_started omits %s: %v", key, rec)
				}
			}
		case "discovery_disabled":
			found = true
		case "discovery_unimplemented":
			t.Errorf("the watcher still reports discovery as unimplemented: %v", rec)
		}
	}
	if !found {
		t.Errorf("the watcher never reported whether its discovery worker is running")
	}
}

// TestF2GracefulTerminationCompleted inspects the log of an application the
// orchestrator has terminated with SIGTERM.
func TestF2GracefulTerminationCompleted(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseDrained)

	lines := readServiceLog(t, e, "renamer-2")
	var sawWithdrawn, sawComplete, sawConsumerStopped bool
	var shutdownSignal string

	for _, ln := range lines {
		var rec map[string]any
		if json.Unmarshal([]byte(ln), &rec) != nil {
			continue
		}
		switch rec["event"] {
		case "readiness_withdrawn":
			sawWithdrawn = true
		case "shutdown_started":
			if s, ok := rec["signal"].(string); ok {
				shutdownSignal = s
			}
		case "shutdown_complete":
			sawComplete = true
			if rec["ready"] != false {
				t.Errorf("shutdown_complete reported ready=%v", rec["ready"])
			}
		case "worker_stopped":
			if rec["worker"] == "consumer" {
				sawConsumerStopped = true
			}
		}
	}

	if shutdownSignal != "terminated" && shutdownSignal != "SIGTERM" {
		t.Errorf("renamer-2 did not record a SIGTERM shutdown; signal=%q", shutdownSignal)
	}
	if !sawWithdrawn {
		t.Errorf("renamer-2 did not withdraw readiness before draining")
	}
	if !sawConsumerStopped {
		t.Errorf("renamer-2 did not report its consumer worker stopping")
	}
	if !sawComplete {
		t.Errorf("renamer-2 did not report shutdown_complete; termination was not graceful")
	}

	exit := strings.TrimSpace(e.LoadState(t, "renamer2-exit-code"))
	if exit != "0" {
		t.Errorf("renamer-2 exited with code %q, expected 0 after SIGTERM", exit)
	}
}
