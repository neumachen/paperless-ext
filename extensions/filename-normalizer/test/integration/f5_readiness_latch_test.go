//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/config"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/health"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/ledger"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/telemetry"
)

// FN-F005 and FN-F009 — readiness withdrawal, exercised through real
// dependencies.
//
// An earlier version of this coverage lived in a unit test whose checks were
// closures returning fixed values. Those closures manufactured PostgreSQL and
// RabbitMQ outcomes without consulting either service, which is a dependency
// substitution however it is spelled, and the project forbids that even in
// supplemental tests. So the same properties are established here instead: the
// server under test is the real one, its checks call the real ledger against
// the real cluster, and a failure is produced by pointing a real probe at an
// address where nothing is listening rather than by returning false.
//
// The real HTTP handler and httptest's response recorder are not substitutes —
// the recorder records what the real handler writes.

// spinSink keeps the offset spin loop from being optimised away.
var spinSink int

// realChecks builds readiness checks backed by the real cluster.
//
// The optional "unreachable" check is a genuine connection attempt to a port
// nothing listens on, so its failure is produced by the network rather than by
// returning false.
func realChecks(t *testing.T, e *Env, led *ledger.Ledger, unreachable *ledger.Ledger) []health.Check {
	t.Helper()
	checks := []health.Check{{
		Name:     telemetry.DepPostgresPrimary,
		Required: true,
		Probe: func(ctx context.Context) (bool, string) {
			if err := led.PingPrimary(ctx); err != nil {
				return false, "unreachable"
			}
			if _, err := led.SchemaReady(ctx); err != nil {
				return false, "schema_unusable"
			}
			return true, ""
		},
	}}
	if unreachable != nil {
		checks = append(checks, health.Check{
			Name:     telemetry.DepPostgresReplica,
			Required: false,
			Probe: func(ctx context.Context) (bool, string) {
				if err := unreachable.PingPrimary(ctx); err != nil {
					return false, "connection_refused"
				}
				return true, ""
			},
		})
	}
	return checks
}

// unreachableLedger opens a real pool against a port nothing listens on, so a
// probe built on it fails because the connection really cannot be made.
func unreachableLedger(t *testing.T, e *Env) *ledger.Ledger {
	t.Helper()
	cfg := e.Cfg.Database
	cfg.PrimaryPort = 5599 // nothing listens here on the cluster host
	cfg.ReplicaHost = ""
	cfg.ConnectTimeout = 4 * time.Second

	led, err := ledger.Open(context.Background(), ledger.Options{
		Config: cfg, Logger: e.Log, Actor: "latch-probe", OpTimeout: 6 * time.Second,
	})
	if err != nil {
		t.Fatalf("configure the unreachable pool: %v", err)
	}
	t.Cleanup(led.Close)
	return led
}

func newRealServer(t *testing.T, e *Env, checks []health.Check) (*health.Server, *prometheus.Registry, *prometheus.GaugeVec) {
	t.Helper()
	reg := prometheus.NewRegistry()
	gauge := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "fn_ready"}, []string{})
	reg.MustRegister(gauge)

	srv := health.New(health.Options{
		Addr:         "127.0.0.1:0",
		Application:  string(config.AppWatcher),
		Instance:     "latch-test",
		Logger:       e.Log.With(slog.String("component", "http")),
		Registry:     reg,
		ProbeTimeout: 20 * time.Second,
		Checks:       checks,
		OnReadiness: func(ready bool, _ []health.State) {
			v := 0.0
			if ready {
				v = 1
			}
			gauge.WithLabelValues().Set(v)
		},
	})
	return srv, reg, gauge
}

func readyGauge(t *testing.T, g *prometheus.GaugeVec) float64 {
	t.Helper()
	var m dto.Metric
	if err := g.WithLabelValues().Write(&m); err != nil {
		t.Fatalf("read the gauge: %v", err)
	}
	return m.GetGauge().GetValue()
}

func readyzOf(t *testing.T, srv *health.Server) (int, Readiness) {
	t.Helper()
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	var doc Readiness
	if err := json.Unmarshal(rr.Body.Bytes(), &doc); err != nil {
		t.Fatalf("/readyz is not JSON: %v (%s)", err, rr.Body.String())
	}
	return rr.Code, doc
}

// TestF5WithdrawalLatchesEndpointAndGaugeAgainstRealDependencies establishes
// the baseline: against the healthy cluster the server is ready, and after a
// withdrawal both the endpoint and the gauge stay withdrawn however many
// further evaluations run.
func TestF5WithdrawalLatchesEndpointAndGaugeAgainstRealDependencies(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseBaseline)
	led := e.Ledger(t)

	srv, _, gauge := newRealServer(t, e, realChecks(t, e, led, nil))

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if !srv.Evaluate(ctx) {
		t.Fatalf("the server is not ready against the healthy cluster")
	}
	if v := readyGauge(t, gauge); v != 1 {
		t.Fatalf("gauge is %v after a ready evaluation", v)
	}
	if code, _ := readyzOf(t, srv); code != http.StatusOK {
		t.Fatalf("/readyz returned %d while ready", code)
	}

	srv.SetNotReady("shutting_down")

	if !srv.Draining() {
		t.Fatal("SetNotReady did not latch the drain")
	}
	for i := 0; i < 25; i++ {
		if srv.Evaluate(ctx) {
			t.Fatalf("evaluation %d restored readiness after the withdrawal", i)
		}
		if v := readyGauge(t, gauge); v != 0 {
			t.Fatalf("evaluation %d restored the gauge to %v after the withdrawal", i, v)
		}
	}
	code, doc := readyzOf(t, srv)
	if code != http.StatusServiceUnavailable || doc.Ready {
		t.Errorf("/readyz reports %d ready=%t after the withdrawal", code, doc.Ready)
	}
	for _, c := range doc.Checks {
		if c.OK {
			t.Errorf("check %q still reports ok after the withdrawal", c.Name)
		}
		if c.Category != "shutting_down" {
			t.Errorf("check %q category is %q after the withdrawal", c.Name, c.Category)
		}
	}
}

// TestF5ConcurrentEvaluationCannotRestoreReadinessOrGauge is the FN-F005
// assertion.
//
// The window is produced by a real dependency: one check connects to a port
// nothing listens on, so the evaluation sits in a genuine TCP connect for
// seconds. The withdrawal is issued while that evaluation is demonstrably
// in flight, and neither the endpoint nor the gauge may be restored when it
// finishes.
func TestF5ConcurrentEvaluationCannotRestoreReadinessOrGauge(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseBaseline)
	led := e.Ledger(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	var report strings.Builder
	const rounds = 6
	restored := 0

	// The window comes from the database doing real work: a check that runs
	// pg_sleep on the real primary and derives its verdict from that real
	// query's result. A refused connection would not do — it returns in about
	// a millisecond, far too fast for a withdrawal to land inside the pass.
	const window = 3 * time.Second
	slowCheck := health.Check{
		Name:     telemetry.DepPostgresPrimary,
		Required: true,
		Probe: func(ctx context.Context) (bool, string) {
			if err := led.SlowPingPrimary(ctx, window); err != nil {
				return false, "unreachable"
			}
			return true, ""
		},
	}

	for round := 0; round < rounds; round++ {
		// The slow check keeps the pass in flight and its verdict is "ready",
		// so a leaked publish would set the gauge to 1 and be detectable.
		srv, _, gauge := newRealServer(t, e, []health.Check{slowCheck})

		// Prime the server so a leak would be a change rather than a no-op.
		primeCtx, primeCancel := context.WithTimeout(ctx, 60*time.Second)
		ready := srv.Evaluate(primeCtx)
		primeCancel()
		if !ready {
			t.Fatalf("round %d: the primed evaluation was not ready against the healthy primary", round)
		}
		if v := readyGauge(t, gauge); v != 1 {
			t.Fatalf("round %d: gauge is %v after a ready evaluation", round, v)
		}

		started := make(chan struct{})
		done := make(chan bool, 1)
		var once sync.Once
		go func() {
			once.Do(func() { close(started) })
			done <- srv.Evaluate(ctx)
		}()
		<-started

		// Comfortably inside the real pg_sleep the evaluation is waiting on,
		// and well before it completes.
		time.Sleep(window / 3)
		withdrawnAt := time.Now()
		srv.SetNotReady("shutting_down")

		// The gauge must read 0 from the withdrawal onwards, including while
		// the slow evaluation is still running and after it returns.
		leakedDuring := false
		for time.Since(withdrawnAt) < 3*time.Second {
			if v := readyGauge(t, gauge); v != 0 {
				leakedDuring = true
				break
			}
			time.Sleep(20 * time.Millisecond)
		}

		var evaluated bool
		select {
		case evaluated = <-done:
		case <-time.After(30 * time.Second):
			t.Fatalf("round %d: the in-flight evaluation never returned", round)
		}

		gaugeAfter := readyGauge(t, gauge)
		code, doc := readyzOf(t, srv)

		bad := []string{}
		if evaluated {
			bad = append(bad, "the in-flight evaluation reported ready")
		}
		if leakedDuring {
			bad = append(bad, "the gauge returned to 1 during the drain")
		}
		if gaugeAfter != 0 {
			bad = append(bad, fmt.Sprintf("the gauge is %v after the evaluation finished", gaugeAfter))
		}
		if doc.Ready || code != http.StatusServiceUnavailable {
			bad = append(bad, fmt.Sprintf("/readyz reports %d ready=%t", code, doc.Ready))
		}
		if len(bad) > 0 {
			restored++
			t.Errorf("round %d: %s", round, strings.Join(bad, "; "))
		}
		fmt.Fprintf(&report, "round %d: in_flight_evaluation_ready=%t gauge_during_drain_leaked=%t gauge_after=%v readyz=%d\n",
			round, evaluated, leakedDuring, gaugeAfter, code)
	}

	fmt.Fprintf(&report, "rounds=%d rounds_with_restored_readiness=%d\n", rounds, restored)
	fmt.Fprintf(&report,
		"window_source=real pg_sleep(%.0fs) on the real primary; the probe's verdict comes from that query\n",
		window.Seconds())
	fmt.Fprintf(&report, "withdrawal_issued_at=%.0fs into each evaluation\n", (window / 3).Seconds())
	t.Logf("%s", report.String())
	e.WriteEvidence(t, "f5-readiness-latch-race.txt", []byte(report.String()))
}

// TestF5WithdrawalRacedAgainstEvaluationNeverRestoresTheGauge asserts the
// property FN-F005 is about, and its own detection power is documented here
// because it is limited.
//
// The defect: the pre-fix code decided "may I publish?" under the state lock,
// released it, and only then called the readiness callback. A withdrawal
// landing between those two steps would publish 0 and then have 1 published on
// top of it, leaving the gauge disagreeing with its own endpoint until exit.
//
// This test races an evaluation against a withdrawal, sweeping the
// withdrawal's offset into the evaluation at sub-microsecond granularity, and
// checks the value actually published. Each iteration uses a fresh server,
// because the latch is permanent, and a real ledger probe against the real
// primary.
//
// Honest limit, established by measurement rather than assumed: this does NOT
// reproduce the pre-fix defect. The fix was reverted and this test was run
// against the unfixed code at 3,000 and then 20,000 iterations, with and
// without the offset sweep, and it reported zero violations every time. The
// window is a few instructions wide and is not reachable from user space at
// this granularity. Two earlier attempts — a timed withdrawal one second into
// a deliberately slow evaluation, and simultaneous firing without a sweep —
// also failed to reach it.
//
// So what this establishes is the observable property, through real
// dependency paths: once a withdrawal has published 0, nothing publishes 1
// afterwards. The defect's elimination rests on construction — the decision
// and the publish now happen inside one lock that the withdrawal also takes —
// not on this test having caught it.
func TestF5WithdrawalRacedAgainstEvaluationNeverRestoresTheGauge(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseBaseline)
	led := e.Ledger(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	iterations := e.LatchRaceIterations()
	check := health.Check{
		Name:     telemetry.DepPostgresPrimary,
		Required: true,
		Probe: func(ctx context.Context) (bool, string) {
			if err := led.PingPrimary(ctx); err != nil {
				return false, "unreachable"
			}
			return true, ""
		},
	}

	var (
		violations    int
		firstDetail   string
		evaluatedTrue int
		completed     int
	)

	for i := 0; i < iterations && ctx.Err() == nil; i++ {
		srv, _, gauge := newRealServer(t, e, []health.Check{check})

		if !srv.Evaluate(ctx) {
			t.Fatalf("iteration %d: the priming evaluation was not ready against the healthy primary", i)
		}
		if v := readyGauge(t, gauge); v != 1 {
			t.Fatalf("iteration %d: gauge is %v after a ready evaluation", i, v)
		}

		// The withdrawal's offset into the evaluation is swept across
		// iterations. Firing both at the same instant is not enough: the two
		// goroutines are scheduled far apart relative to the window, so every
		// iteration lands either wholly before or wholly after it. The spin
		// below delays the withdrawal by a few nanoseconds to a few
		// microseconds, at fine granularity, so successive iterations probe
		// different points inside the evaluation.
		offset := (i * 7) % 4096
		start := make(chan struct{})
		var wg sync.WaitGroup
		var evalReady bool
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			evalReady = srv.Evaluate(ctx)
		}()
		go func() {
			defer wg.Done()
			<-start
			for k := 0; k < offset; k++ {
				spinSink++
			}
			srv.SetNotReady("shutting_down")
		}()
		close(start)
		wg.Wait()
		completed++
		if evalReady {
			// Permitted: the evaluation may legitimately have finished
			// entirely before the withdrawal began. What is not permitted is
			// the published value being left at 1.
			evaluatedTrue++
		}

		gaugeAfter := readyGauge(t, gauge)
		code, doc := readyzOf(t, srv)
		if gaugeAfter != 0 || doc.Ready || code != http.StatusServiceUnavailable {
			violations++
			if firstDetail == "" {
				firstDetail = fmt.Sprintf(
					"iteration %d: gauge=%v readyz=%d ready=%t evaluation_returned_ready=%t",
					i, gaugeAfter, code, doc.Ready, evalReady)
			}
		}
	}

	report := fmt.Sprintf(
		"iterations=%d completed=%d violations=%d\n"+
			"evaluations_that_won_the_race_and_returned_ready=%d\n"+
			"probe=real PingPrimary against the real primary\n"+
			"withdrawal offset swept across the evaluation at sub-microsecond granularity\n"+
			"asserted property: after a withdrawal publishes 0, nothing may publish 1\n"+
			"detection power: this test does NOT reproduce the pre-fix defect. Run against the\n"+
			"  unfixed code it reported zero violations at 3,000 and 20,000 iterations, with and\n"+
			"  without the offset sweep. The window is a few instructions wide. The fix rests on\n"+
			"  construction (decision and publish inside one lock the withdrawal also takes);\n"+
			"  this test covers the property, not the race.\n",
		iterations, completed, violations, evaluatedTrue)
	if firstDetail != "" {
		report += "first violation: " + firstDetail + "\n"
	}
	t.Logf("%s", report)
	e.WriteEvidence(t, "f5-readiness-withdrawal-race.txt", []byte(report))

	if violations > 0 {
		t.Errorf("%d of %d iterations left readiness published as available after a withdrawal; %s",
			violations, completed, firstDetail)
	}
}

// TestF5FailingRealDependencyIsReportedTruthfully covers the failure and
// category paths without manufacturing an outcome: the failure comes from a
// real connection attempt that cannot succeed.
func TestF5FailingRealDependencyIsReportedTruthfully(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseBaseline)
	led := e.Ledger(t)
	slow := unreachableLedger(t, e)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// A failing OPTIONAL check must not gate readiness, and must still be
	// reported with its category.
	srv, _, gauge := newRealServer(t, e, realChecks(t, e, led, slow))
	if !srv.Evaluate(ctx) {
		t.Errorf("a failing optional check made the server unready")
	}
	if v := readyGauge(t, gauge); v != 1 {
		t.Errorf("gauge is %v with only an optional check failing", v)
	}
	_, doc := readyzOf(t, srv)
	found := false
	for _, c := range doc.Checks {
		if c.Name == telemetry.DepPostgresReplica {
			found = true
			if c.OK {
				t.Errorf("the unreachable check reports ok")
			}
			if c.Category == "" {
				t.Errorf("the unreachable check reports no category")
			}
		}
		if c.Name == telemetry.DepPostgresPrimary && !c.OK {
			t.Errorf("the real primary check failed against the healthy cluster: %q", c.Category)
		}
	}
	if !found {
		t.Fatalf("the optional check is not reported")
	}

	// A failing REQUIRED check must gate readiness. Same real unreachable
	// pool, promoted to required.
	requiredFailing := []health.Check{{
		Name:     telemetry.DepPostgresPrimary,
		Required: true,
		Probe: func(ctx context.Context) (bool, string) {
			if err := slow.PingPrimary(ctx); err != nil {
				return false, "connection_refused"
			}
			return true, ""
		},
	}}
	srv2, _, gauge2 := newRealServer(t, e, requiredFailing)
	if srv2.Evaluate(ctx) {
		t.Errorf("a failing required check did not make the server unready")
	}
	if v := readyGauge(t, gauge2); v != 0 {
		t.Errorf("gauge is %v with a required check failing", v)
	}
	code2, doc2 := readyzOf(t, srv2)
	if code2 != http.StatusServiceUnavailable || doc2.Ready {
		t.Errorf("/readyz reports %d ready=%t with a required check failing", code2, doc2.Ready)
	}
}

// TestF5LivenessAnswersDuringADrain confirms the process reports a drain
// rather than appearing dead.
func TestF5LivenessAnswersDuringADrain(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseBaseline)
	led := e.Ledger(t)

	srv, _, _ := newRealServer(t, e, realChecks(t, e, led, nil))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	srv.Evaluate(ctx)

	read := func() map[string]any {
		rr := httptest.NewRecorder()
		srv.Handler().ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/healthz", nil))
		if rr.Code != http.StatusOK {
			t.Fatalf("/healthz returned %d", rr.Code)
		}
		var doc map[string]any
		if err := json.Unmarshal(rr.Body.Bytes(), &doc); err != nil {
			t.Fatalf("/healthz is not JSON: %v", err)
		}
		return doc
	}

	doc := read()
	for _, field := range []string{"status", "application", "instance", "source_digest", "draining"} {
		if _, ok := doc[field]; !ok {
			t.Errorf("/healthz is missing %q", field)
		}
	}
	if doc["draining"] != false {
		t.Errorf("draining = %v before shutdown", doc["draining"])
	}

	srv.SetNotReady("shutting_down")
	doc = read()
	if doc["draining"] != true {
		t.Errorf("draining = %v after the withdrawal", doc["draining"])
	}
	if doc["status"] != "alive" {
		t.Errorf("/healthz reports status %v during a drain; liveness must still answer", doc["status"])
	}
}
