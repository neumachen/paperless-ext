//go:build integration

package integration

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/broker"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/jobs"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/ledger"
)

// FN-F001 — a broker interruption must not strand the publisher.
//
// The publisher drains stale unroutable returns before each publication. The
// AMQP client closes that notification channel when the connection or channel
// closes, and a closed Go channel is permanently ready to receive, so a drain
// loop that ignores the "open" result spins instead of terminating. Because
// the dispatch worker also performs stranded-claim recovery, a publisher stuck
// there would stop claim recovery too, and reconnecting the broker would not
// restore progress.
//
// These assertions drive the real publisher against the real broker with the
// connection genuinely closed underneath it.

// TestF4PublishReturnsWhenTheConnectionClosesUnderIt interrupts the
// application's own connection while publications are actively in flight.
//
// A previous version of this closed the connection and *then* called Publish.
// Every attempt therefore failed at channel acquisition with "broker
// connection is not established" — before the stale-return drain that the fix
// is about. That established reconnection, not the corrected path, and it must
// not be described as reproducing the original interleaving.
//
// This instead runs a publisher loop and a closer loop concurrently, so closes
// land at arbitrary points inside publications, and classifies every outcome
// by the path it reached. The assertion is that at least one attempt got past
// channel acquisition — that is what makes the drain and confirm-wait paths
// reachable at all — and the distribution is recorded so the evidence says
// which paths were actually exercised rather than implying one.
func TestF4PublishReturnsWhenTheConnectionClosesUnderIt(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseBrokerTorn)

	conn, _ := e.Broker(t)
	top := e.DeclareIsolated(t, conn, "torn")
	pub := broker.NewPublisher(conn, top, e.Log, e.Cfg.Broker.ConfirmTimeout)
	t.Cleanup(pub.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	// Warm the publisher so it holds a live channel with a live return
	// notification channel, which is the state the defect needs.
	if result, err := pub.Publish(ctx, jobs.Message{
		ContractVersion: jobs.ContractVersion,
		JobID:           "a1a1a1a1-0000-4000-8000-000000000001",
		Attempt:         1, EnqueuedAt: time.Now().UTC(),
	}); result != broker.PublishConfirmed {
		t.Fatalf("warm-up publish: %s (%v)", result, err)
	}

	// The publisher's own bound is the confirm timeout. A spinning drain would
	// never reach it, so a call taking materially longer than a generous
	// multiple of it has not returned.
	budget := 4 * e.Cfg.Broker.ConfirmTimeout
	if budget < 20*time.Second {
		budget = 20 * time.Second
	}

	type attempt struct {
		path string
		took time.Duration
	}

	var (
		mu       sync.Mutex
		attempts []attempt
		slowest  time.Duration
	)

	// classify maps an outcome onto the furthest point in Publish it reached.
	classify := func(result broker.PublishResult, err error) string {
		msg := ""
		if err != nil {
			msg = err.Error()
		}
		switch {
		case result == broker.PublishConfirmed:
			return "confirmed"
		case result == broker.PublishReturned:
			return "returned_unroutable"
		case strings.Contains(msg, "return channel closed before publishing"):
			return "drain_found_channel_closed"
		case strings.Contains(msg, "return channel closed before routability"):
			return "confirmed_then_return_channel_closed"
		case result == broker.PublishUnconfirmed:
			return "unconfirmed_after_publish"
		case strings.Contains(msg, "not established"):
			return "channel_acquisition_refused"
		case result == broker.PublishError:
			return "publish_error_after_acquisition"
		default:
			return "other"
		}
	}

	// Paths that are only reachable after the channel was acquired, i.e. at or
	// beyond the drain the fix changed.
	postAcquisition := map[string]bool{
		"confirmed":                            true,
		"returned_unroutable":                  true,
		"drain_found_channel_closed":           true,
		"confirmed_then_return_channel_closed": true,
		"unconfirmed_after_publish":            true,
		"publish_error_after_acquisition":      true,
	}

	stop := make(chan struct{})
	var closer sync.WaitGroup
	closer.Add(1)
	go func() {
		defer closer.Done()
		// Real interruptions of the application's own connection, repeatedly,
		// with no coordination with the publisher.
		for {
			select {
			case <-stop:
				return
			case <-time.After(120 * time.Millisecond):
				conn.ForceClose()
			}
		}
	}()

	const publications = 400
	for i := 0; i < publications && ctx.Err() == nil; i++ {
		start := time.Now()
		done := make(chan attempt, 1)
		go func(i int) {
			result, err := pub.Publish(ctx, jobs.Message{
				ContractVersion: jobs.ContractVersion,
				JobID:           fmt.Sprintf("a1a1a1a1-0000-4000-8000-%012d", i),
				Attempt:         1, EnqueuedAt: time.Now().UTC(),
			})
			done <- attempt{path: classify(result, err), took: time.Since(start)}
		}(i)

		select {
		case a := <-done:
			mu.Lock()
			attempts = append(attempts, a)
			if a.took > slowest {
				slowest = a.took
			}
			mu.Unlock()
		case <-time.After(budget):
			close(stop)
			closer.Wait()
			t.Fatalf("publication %d did not return within %s while the connection was being closed under it; "+
				"the dispatch worker is stranded", i, budget)
		}
	}
	close(stop)
	closer.Wait()

	mu.Lock()
	observed := map[string]int{}
	for _, a := range attempts {
		observed[a.path]++
	}
	total := len(attempts)
	slowestSeen := slowest
	mu.Unlock()

	paths := make([]string, 0, len(observed))
	for p := range observed {
		paths = append(paths, p)
	}
	sort.Strings(paths)

	var report strings.Builder
	fmt.Fprintf(&report, "publications=%d all_returned=true slowest_return=%s budget=%s confirm_timeout=%s\n",
		total, slowestSeen.Round(time.Millisecond), budget, e.Cfg.Broker.ConfirmTimeout)
	fmt.Fprintf(&report, "closer=real ForceClose of the application's own connection every 120ms, uncoordinated\n")
	fmt.Fprintf(&report, "outcome distribution by furthest path reached:\n")
	postCount := 0
	for _, p := range paths {
		marker := ""
		if postAcquisition[p] {
			marker = "  (past channel acquisition)"
			postCount += observed[p]
		}
		fmt.Fprintf(&report, "  %-38s %5d%s\n", p, observed[p], marker)
	}
	fmt.Fprintf(&report, "attempts_past_channel_acquisition=%d\n", postCount)

	if total < publications/2 {
		t.Errorf("only %d of %d publications completed", total, publications)
	}
	if postCount == 0 {
		t.Errorf("every attempt was refused at channel acquisition, so no attempt reached the "+
			"stale-return drain or the confirm wait; this run does not exercise the corrected path:\n%s",
			report.String())
	}
	// The whole point: nothing hung.
	if slowestSeen > budget {
		t.Errorf("the slowest publication took %s, beyond the %s budget", slowestSeen, budget)
	}

	// And the publisher must work again afterwards.
	if err := waitForErr(90*time.Second, func() error {
		r, perr := pub.Publish(ctx, jobs.Message{
			ContractVersion: jobs.ContractVersion,
			JobID:           "a1a1a1a1-0000-4000-8000-0000000000ff",
			Attempt:         1, EnqueuedAt: time.Now().UTC(),
		})
		if r != broker.PublishConfirmed {
			return fmt.Errorf("publish result %s: %v", r, perr)
		}
		return nil
	}); err != nil {
		t.Fatalf("the publisher never recovered after the interruptions: %v", err)
	}
	fmt.Fprintf(&report, "recovered_after_interruptions=confirmed\n")

	t.Logf("%s", report.String())
	e.WriteEvidence(t, "f4-publish-under-connection-loss.txt", []byte(report.String()))
}

// TestF4WatcherResumesDispatchAndClaimRecoveryAfterBrokerInterruption is the
// application-level half of FN-F001.
//
// A job is left stranded in the dispatching state, as a watcher that died
// between claiming and confirming would leave it. The broker is then torn down
// under the running watcher by the orchestrator. Afterwards the same watcher
// process — not a replacement — must both reclaim the stranded job and dispatch
// it through to a durable outcome.
func TestF4WatcherResumesDispatchAndClaimRecoveryAfterBrokerInterruption(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseBrokerTorn)
	led := e.Ledger(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Strand a job: claim it into dispatching and never publish it. This is
	// the real claim path the watcher uses, so the row is indistinguishable
	// from one left behind by a watcher that died mid-dispatch.
	stranded := registerSyntheticJob(t, led, e, "f1-stranded-claim")
	var claimed []ledger.Job
	err := waitForErr(60*time.Second, func() error {
		got, cerr := led.ClaimForDispatch(ctx, 8)
		if cerr != nil {
			return cerr
		}
		claimed = append(claimed, got...)
		for _, j := range claimed {
			if j.JobID == stranded.JobID {
				return nil
			}
		}
		return fmt.Errorf("the watcher claimed %d job(s) before this test could; retrying", len(got))
	})
	if err != nil {
		// The watcher may legitimately have claimed and dispatched it first.
		// In that case the stranded-claim half cannot be set up, which is
		// reported rather than passed over.
		cur, gerr := led.GetJob(ctx, stranded.JobID)
		t.Fatalf("could not strand a claim for %s (state now %v, err %v): %v",
			stranded.JobID, cur.State, gerr, err)
	}

	cur, err := led.GetJob(ctx, stranded.JobID)
	if err != nil {
		t.Fatalf("read the stranded job: %v", err)
	}
	if cur.State != jobs.StateDispatching {
		t.Fatalf("job %s is in state %q, expected it to be stranded in %q",
			stranded.JobID, cur.State, jobs.StateDispatching)
	}
	if cur.ClaimedBy == nil || !strings.HasPrefix(*cur.ClaimedBy, "integration/") {
		t.Fatalf("the stranded claim is not held by this test: %v", cur.ClaimedBy)
	}
	t.Logf("job %s is stranded in %q, claimed by %q", cur.JobID, cur.State, *cur.ClaimedBy)

	// Also register an ordinary job, so the test observes normal dispatch
	// resuming as well as recovery.
	fresh := registerSyntheticJob(t, led, e, "f1-post-interruption")

	// The orchestrator has already torn the broker down and brought it back
	// for this phase. Both jobs must now reach a durable outcome through the
	// same watcher process.
	claimMaxAge := 30 * time.Second
	budget := 4*claimMaxAge + 90*time.Second

	for _, target := range []struct {
		id   string
		what string
	}{{stranded.JobID, "stranded claim"}, {fresh.JobID, "job registered after the interruption"}} {
		id, what := target.id, target.what
		werr := waitForErr(budget, func() error {
			j, gerr := led.GetJob(ctx, id)
			if gerr != nil {
				return gerr
			}
			if j.State != jobs.StateHeld {
				return fmt.Errorf("%s (%s) is in state %q", what, id, j.State)
			}
			if j.DispatchedAt == nil {
				return fmt.Errorf("%s (%s) was settled with no recorded dispatch", what, id)
			}
			return nil
		})
		if werr != nil {
			t.Fatalf("dispatch did not resume after the broker interruption: %v", werr)
		}
	}

	// The reclaim must be visible in the retained history, which is what
	// distinguishes real recovery from the job simply having been re-dispatched.
	events, err := led.Events(ctx, stranded.JobID)
	if err != nil {
		t.Fatalf("read retained history: %v", err)
	}
	var types []string
	reclaimed := false
	for _, ev := range events {
		types = append(types, string(ev.EventType))
		if ev.EventType == jobs.EventDispatchReclaimed {
			reclaimed = true
		}
	}
	if !reclaimed {
		t.Errorf("job %s reached an outcome without a %q event; claim recovery is not what unblocked it: %v",
			stranded.JobID, jobs.EventDispatchReclaimed, types)
	}

	strandedFinal, _ := led.GetJob(ctx, stranded.JobID)
	freshFinal, _ := led.GetJob(ctx, fresh.JobID)

	// And the watcher must be the same process throughout: a restart would
	// make this prove nothing about resumption.
	restarts := strings.TrimSpace(e.LoadState(t, "watcher-restart-count"))
	if restarts != "0" {
		t.Errorf("the watcher container restarted %s time(s) during this phase, so resumption by the same process is not established", restarts)
	}

	report := fmt.Sprintf(
		"stranded_job=%s state=%s dispatch_attempts=%d history=%s reclaim_observed=%t\n"+
			"post_interruption_job=%s state=%s dispatch_attempts=%d\n"+
			"watcher_container_restarts_during_phase=%s\n",
		strandedFinal.JobID, strandedFinal.State, strandedFinal.DispatchAttempts,
		strings.Join(types, ","), reclaimed,
		freshFinal.JobID, freshFinal.State, freshFinal.DispatchAttempts, restarts)
	t.Logf("%s", report)
	e.WriteEvidence(t, "f4-dispatch-resumed-after-interruption.txt", []byte(report))
}

// TestF4WatcherShutdownRemainsBoundedAfterInterruption confirms the third part
// of the FN-F001 outcome: the watcher's own termination stays bounded after
// the interruption, rather than hanging on a stuck publisher.
func TestF4WatcherShutdownRemainsBoundedAfterInterruption(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseWatcherDrained)

	raw := strings.TrimSpace(e.LoadState(t, "watcher-stop-seconds"))
	code := strings.TrimSpace(e.LoadState(t, "watcher-exit-code"))

	secs, err := time.ParseDuration(raw + "s")
	if err != nil {
		t.Fatalf("the recorded watcher stop duration is not a number: %q", raw)
	}
	if code != "0" {
		t.Errorf("the watcher exited with code %q after SIGTERM, expected 0", code)
	}
	// Compose was given a 40s grace period; a stranded publisher would have
	// consumed it and forced a SIGKILL.
	if secs >= 35*time.Second {
		t.Errorf("the watcher took %s to terminate, which is at or beyond its grace period: shutdown is not bounded", secs)
	}

	records, _ := parseJSONLog(readServiceLog(t, e, "watcher"))
	var sawComplete, sawTimeout bool
	for _, r := range records {
		switch r.String("event") {
		case "shutdown_complete":
			sawComplete = true
		case "shutdown_timeout":
			sawTimeout = true
		}
	}
	if !sawComplete {
		t.Errorf("the watcher never logged shutdown_complete")
	}
	if sawTimeout {
		t.Errorf("the watcher logged shutdown_timeout: a worker did not stop within the budget")
	}
	t.Logf("watcher terminated in %s with exit code %s after the broker interruption", secs, code)
	e.WriteEvidence(t, "f4-watcher-shutdown-bounded.txt", []byte(fmt.Sprintf(
		"stop_duration_seconds=%s exit_code=%s shutdown_complete=%t shutdown_timeout_logged=%t\n",
		raw, code, sawComplete, sawTimeout)))
}
