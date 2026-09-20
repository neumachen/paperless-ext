//go:build integration

package integration

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/broker"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/jobs"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/ledger"
)

// FN-F005 — graceful shutdown with work genuinely in flight.
//
// The previous shutdown assertion terminated an idle renamer and looked for
// log events. That establishes an orderly exit, not the stronger claim: that a
// delivery already taken reaches a durable outcome, that readiness stays
// withdrawn while it does, and that the process still exits within its budget.
//
// The drain order this exercises is the one the supervisor now enforces:
// readiness is withdrawn first, service workers are cancelled and waited for
// next, and the broker connection — which a settling delivery needs in order
// to acknowledge — is torn down only afterwards. In-flight handlers run on a
// context detached from the cancelled consume context, so shutdown stops new
// deliveries without aborting the durable write of work already accepted.

// TestF5DrainLoadFillsTheWorkQueue makes the renamers genuinely saturated
// before the orchestrator terminates one of them.
//
// Registering jobs and leaving them to the watcher does not work: the
// dispatcher publishes a bounded batch per interval (32 per 500ms by default),
// so the queue stays nearly empty, the renamers are starved, and a SIGTERM
// finds nothing in flight. A first attempt at this phase registered 85,201
// jobs and still drained in two milliseconds with an idle consumer, which is
// an idle exit rather than a drain.
//
// So the jobs are registered and then published directly, through the
// application's own publisher onto the application's own work queue, with
// publisher confirms. Nothing is substituted: it is the real publisher, the
// real exchange and the real queue.
//
// The orchestrator also stops both renamers before this phase runs. That makes
// the backlog deterministic instead of a race between publication and
// consumption: publishing into a queue with live consumers simply hands each
// message straight to one of them, so no depth accumulates however fast the
// publisher goes. With the consumers stopped, the queue reaches the full batch,
// the orchestrator starts them again, and renamer-2 is then terminated while
// it is demonstrably working through a real backlog.
//
// The phase confirms the depth from the broker's own management API before it
// returns, so the orchestrator never relies on a sleep for this.
func TestF5DrainLoadFillsTheWorkQueue(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseDrainUnderLoad)
	led := e.Ledger(t)

	conn, _ := e.Broker(t)
	pub := broker.NewPublisher(conn, broker.TopologyFromConfig(e.Cfg.Broker), e.Log, e.Cfg.Broker.ConfirmTimeout)
	t.Cleanup(pub.Close)

	batch, copies := e.DrainBatchSize(), e.DrainCopies()
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	// Registration is the slow half, so the depth comes from publishing each
	// job's reference several times rather than from registering more jobs.
	//
	// That is deliberate and within the contract, not a trick to inflate a
	// number: duplicate messages are explicitly allowed and their processing
	// is required to be safe. Each duplicate is settled against the hold the
	// first delivery recorded, so this also exercises duplicate-delivery
	// safety at a scale the other assertions do not reach. It is what makes
	// the backlog last long enough for a termination to land inside it —
	// a batch of roughly a thousand drains in well under a second.
	ids := make([]string, 0, batch)
	start := time.Now()
	for i := 0; i < batch; i++ {
		size := int64(64)
		algo := "sha256"
		sum := sha256.Sum256([]byte(fmt.Sprintf("drain:%s:%d", e.RunID, i)))
		job, err := led.RegisterJob(ctx, ledger.RegisterInput{
			SourceRoot:      e.Cfg.Storage.Incoming,
			SourceName:      fmt.Sprintf("synthetic %s f5-drain-%05d.pdf", e.RunID, i),
			SizeBytes:       &size,
			FingerprintAlgo: &algo,
			Fingerprint:     sum[:],
			PolicyIdentity:  e.Cfg.Policy.Identity,
		})
		if err != nil {
			t.Fatalf("register job %d: %v", i, err)
		}
		ids = append(ids, job.JobID)
	}
	registeredAt := time.Now()
	t.Logf("registered %d jobs in %s", len(ids), registeredAt.Sub(start).Round(time.Millisecond))

	published := 0
	for copy := 0; copy < copies; copy++ {
		for _, id := range ids {
			result, perr := pub.Publish(ctx, jobs.Message{
				ContractVersion: jobs.ContractVersion,
				JobID:           id,
				Attempt:         copy + 1,
				EnqueuedAt:      time.Now().UTC(),
			})
			if result != broker.PublishConfirmed {
				t.Fatalf("publish %s (copy %d): %s (%v)", id, copy+1, result, perr)
			}
			published++
		}
	}
	t.Logf("published %d messages for %d jobs (%d copies each) in %s",
		published, len(ids), copies, time.Since(registeredAt).Round(time.Millisecond))

	// The deterministic signal: the broker itself reports the backlog. The
	// consumers are stopped for this phase, so the depth is the batch.
	var st QueueStats
	minBacklog := published * 3 / 4
	if err := waitForErr(90*time.Second, func() error {
		st = e.QueueStats(t, e.Cfg.Broker.Queue)
		if st.Consumers != 0 {
			return fmt.Errorf("the broker reports %d consumer(s) on %s; the orchestrator must stop the renamers "+
				"before this phase, otherwise no backlog can accumulate", st.Consumers, e.Cfg.Broker.Queue)
		}
		if st.Ready+st.Unacknowledged < minBacklog {
			return fmt.Errorf("the broker reports only %d ready and %d unacknowledged message(s), below the %d expected",
				st.Ready, st.Unacknowledged, minBacklog)
		}
		return nil
	}); err != nil {
		t.Fatalf("the work queue never held the published backlog: %v", err)
	}
	t.Logf("broker backlog with consumers stopped: ready=%d unacknowledged=%d consumers=%d",
		st.Ready, st.Unacknowledged, st.Consumers)

	sample := make([]string, 0, 10)
	for i := 0; i < len(ids) && i < 10; i++ {
		sample = append(sample, ids[i])
	}
	e.SaveState(t, "drain-registered", strconv.Itoa(len(ids)))
	e.SaveState(t, "drain-published", strconv.Itoa(published))
	e.SaveState(t, "drain-sample-ids", strings.Join(sample, ","))
	// Every id, not only a sample. The drained phase has to account for the
	// complete fixture set; a sample cannot show that the rest finished.
	e.SaveState(t, "drain-all-ids", strings.Join(ids, ","))
	e.SaveState(t, "drain-backlog", strconv.Itoa(st.Ready+st.Unacknowledged))

	e.WriteEvidence(t, "f5-drain-load.txt", []byte(fmt.Sprintf(
		"registered_jobs=%d copies_per_job=%d published_messages=%d\n"+
			"publish_oracle=publisher_confirms total_duration=%s\n"+
			"consumers_stopped_while_filling=true\n"+
			"broker_backlog_ready=%d broker_backlog_unacknowledged=%d broker_consumers=%d\n"+
			"note: the duplicates are contract-permitted and each is settled against the hold the\n"+
			"      first delivery recorded, so this also exercises duplicate-delivery safety at scale.\n",
		len(ids), copies, published, time.Since(start).Round(time.Millisecond),
		st.Ready, st.Unacknowledged, st.Consumers)))
}

// TestF5DrainUnderLoadReachedDurableOutcomes is the assertion after the
// orchestrator terminated a renamer mid-batch.
// splitIDs turns the comma-joined state value back into ids, dropping the
// empty element an empty or trailing-comma value would otherwise produce.
func splitIDs(v string) []string {
	var out []string
	for _, p := range strings.Split(v, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func TestF5DrainUnderLoadReachedDurableOutcomes(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseDrained)
	led := e.Ledger(t)

	registered, err := strconv.Atoi(strings.TrimSpace(e.LoadState(t, "drain-registered")))
	if err != nil {
		t.Fatalf("unreadable registered count: %v", err)
	}
	sample := strings.Split(e.LoadState(t, "drain-sample-ids"), ",")

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	// Every job must reach a durable outcome. A delivery interrupted by the
	// shutdown is allowed to be redelivered and settled by the surviving
	// instance; what is not allowed is a job left without an outcome. The
	// check is aggregate because the load is thousands of rows: no job may
	// remain in any non-terminal state.
	//
	// DERIVED from jobs.IsTerminal rather than listed. The list here named
	// four states and omitted `publishing`, so a fixture stopped between its
	// claim and its link -- exactly where this phase's injected hold puts one
	// -- counted as having reached a durable outcome. Deriving it means a
	// state added to the model cannot be forgotten here.
	var nonTerminal []jobs.State
	for _, st := range jobs.States() {
		if !jobs.IsTerminal(st) {
			nonTerminal = append(nonTerminal, st)
		}
	}
	// Scoped to THIS phase's work, not to the whole table.
	//
	// The aggregate snapshot counts every row the ledger has ever held. A job
	// left `publishing` by an unrelated exercise twenty-one hours earlier --
	// one this pass is explicitly not allowed to resolve -- therefore failed
	// the drain, reporting `publishing=1` about work the drain never touched.
	// The question is whether the terminated instance stranded ITS work.
	//
	// This phase's jobs are the synthetic load, whose source names carry the
	// run id, and the real drain documents, whose prefix the orchestrator
	// records alongside them.
	realPrefix := strings.TrimSpace(e.LoadState(t, "drain-real-prefix"))
	mine := func(sourceName string) bool {
		if strings.Contains(sourceName, e.RunID) {
			return true
		}
		return realPrefix != "" && strings.HasPrefix(sourceName, realPrefix)
	}
	var snap ledger.Counts
	werr := waitForErr(6*time.Minute, func() error {
		var serr error
		snap, serr = led.Snapshot(ctx)
		if serr != nil {
			return serr
		}
		var outstanding []string
		for _, st := range nonTerminal {
			if snap.ByState[st] == 0 {
				continue
			}
			// The whole state, not its oldest 5000. JobsInState orders by
			// updated_at ASC and truncates, so a fixed bound silently drops
			// the newest rows -- which are exactly this run's. The snapshot
			// already says how many there are; the margin covers rows that
			// arrive between the two reads.
			want := snap.ByState[st] + 256
			rows, lerr := led.JobsInState(ctx, st, want)
			if lerr != nil {
				// An inspection that could not run is unknown, and unknown is
				// not "none outstanding".
				return fmt.Errorf("listing jobs in state %s: %w", st, lerr)
			}
			if len(rows) >= want {
				return fmt.Errorf("state %s returned %d rows at the bound %d; "+
					"the listing may be truncated and cannot establish that none are outstanding",
					st, len(rows), want)
			}
			n := 0
			for _, j := range rows {
				if mine(j.SourceName) {
					n++
				}
			}
			if n > 0 {
				outstanding = append(outstanding, fmt.Sprintf("%s=%d", st, n))
			}
		}
		if len(outstanding) > 0 {
			return fmt.Errorf("jobs from this phase are still outstanding: %s",
				strings.Join(outstanding, " "))
		}
		return nil
	})
	if werr != nil {
		t.Fatalf("jobs were left without a durable outcome after a renamer was terminated under load: %v", werr)
	}
	// Unrelated outstanding work is reported, never asserted on: it belongs to
	// whatever produced it, and this phase may not resolve it.
	var foreignOutstanding []string
	for _, st := range nonTerminal {
		if n := snap.ByState[st]; n > 0 {
			foreignOutstanding = append(foreignOutstanding, fmt.Sprintf("%s=%d", st, n))
		}
	}
	if len(foreignOutstanding) > 0 {
		t.Logf("outstanding jobs elsewhere in the ledger, not this phase's and not asserted on: %s",
			strings.Join(foreignOutstanding, " "))
	}

	// A spot check on individually recorded identities, so the aggregate is
	// not the only oracle.
	for _, id := range sample {
		if id == "" {
			continue
		}
		j, gerr := led.GetJob(ctx, id)
		if gerr != nil {
			t.Errorf("sampled job %s is not readable: %v", id, gerr)
			continue
		}
		if j.State != jobs.StateHeld {
			t.Errorf("sampled job %s is in state %q, expected %q", id, j.State, jobs.StateHeld)
		}
	}

	// Both of the assertions that used to live here read the WHOLE ledger.
	//
	// `processing != 0` failed on any job another exercise left mid-flight,
	// and `held >= registered` passed on a ledger already holding tens of
	// thousands of held rows from months of runs -- it could not have failed.
	// Neither said anything about this run.
	//
	// Every id registered by the load is checked individually instead. These
	// jobs are registered with no source file, so `held` is the only outcome
	// they can legitimately reach, and there are a few hundred of them.
	allIDs := splitIDs(e.LoadState(t, "drain-all-ids"))
	if len(allIDs) != registered {
		t.Fatalf("recorded %d job ids for a load that registered %d; the fixture set is incomplete",
			len(allIDs), registered)
	}
	notHeld := 0
	unreadable := 0
	for _, id := range allIDs {
		j, gerr := led.GetJob(ctx, id)
		if gerr != nil {
			unreadable++
			continue
		}
		if j.State != jobs.StateHeld {
			notHeld++
			if notHeld <= 5 {
				t.Errorf("load job %s is in state %q, expected %q", id, j.State, jobs.StateHeld)
			}
		}
	}
	if unreadable != 0 {
		t.Errorf("%d of %d load jobs could not be read; unknown is not a durable outcome",
			unreadable, len(allIDs))
	}
	if notHeld != 0 {
		t.Errorf("%d of %d load jobs did not reach %q", notHeld, len(allIDs), jobs.StateHeld)
	}
	// There used to be a whole-ledger assertion here that no job is ever
	// delivered or uncertain. Both states are reachable now, and the
	// normalization phase legitimately produces deliveries, so that check
	// would fail on the system working. The property it was protecting -- that
	// these particular jobs, registered with no source file, cannot be
	// delivered -- is already covered by the per-job sample loop above, which
	// requires each of them to be held.

	// The terminated instance must have settled work after shutdown began, or
	// nothing was in flight and the drain was not exercised.
	since := e.Since(t, "drain")
	records := recordsSince(t, e, "renamer-2", since)

	var (
		shutdownAt      time.Time
		completedAt     time.Time
		settledAfter    int
		drainingCount   int
		sawDraining     bool
		sawComplete     bool
		sawTimeout      bool
		inFlightAtDrain int
		faultPaused     int
	)
	for _, r := range records {
		ts, perr := time.Parse(time.RFC3339Nano, r.String("time"))
		if perr != nil {
			continue
		}
		switch r.String("event") {
		case "fault_point_paused":
			// The instance actually ENTERED the injected hold. Being armed is
			// not evidence of that: a run armed with hold_after_claim took
			// 5213 deliveries and paused none, because the synthetic drain
			// load is rejected at source validation and never reaches the
			// publication path where every fault point lives.
			faultPaused++
		case "shutdown_started":
			if shutdownAt.IsZero() {
				shutdownAt = ts
			}
		case "readiness_withdrawn":
			if v, ok := r.Fields["count"].(float64); ok {
				inFlightAtDrain = int(v)
			}
		case "consumer_draining":
			sawDraining = true
			if v, ok := r.Fields["count"].(float64); ok && int(v) > drainingCount {
				drainingCount = int(v)
			}
		case "delivery_settled":
			// Bounded to the shutdown itself. Counting everything at or after
			// shutdown_started would also count deliveries the instance
			// handled after the orchestrator restarted it.
			if !shutdownAt.IsZero() && !ts.Before(shutdownAt) && completedAt.IsZero() {
				settledAfter++
			}
		case "shutdown_complete":
			sawComplete = true
			if completedAt.IsZero() {
				completedAt = ts
			}
		case "shutdown_timeout":
			sawTimeout = true
		}
	}

	if shutdownAt.IsZero() {
		t.Fatalf("renamer-2 never logged shutdown_started within the drain interval")
	}
	if !sawComplete {
		t.Errorf("renamer-2 never logged shutdown_complete")
	}
	if sawTimeout {
		t.Errorf("renamer-2 logged shutdown_timeout: a worker did not finish within the budget")
	}
	// The whole point of the phase, asserted as separate necessary facts
	// rather than as "any one of three looked non-zero".
	//
	// The old disjunction passed whenever the phase happened to catch a single
	// delivery, which it did on three runs out of four, twice with identical
	// evidence. A property proven by whichever way a race falls is not proven.
	//
	// 1. The fixture reached the interruption point.
	if faultPaused < 1 {
		t.Errorf("renamer-2 never entered its injected hold (fault_point_paused=%d): "+
			"the termination did not land on work in progress, so this run did not "+
			"exercise a drain however the counters below read", faultPaused)
	}
	// 2. Work was genuinely active when shutdown began and reached its durable
	//    outcome DURING the shutdown. This is the drain itself.
	if settledAfter < 1 {
		t.Errorf("no delivery settled at or after shutdown_started "+
			"(settled_after_shutdown=%d): nothing drained", settledAfter)
	}
	// 3. Readiness was withdrawn while work was still held. Either the
	//    instance reported draining, or it recorded the in-flight count at
	//    withdrawal; both are its own observation of the same moment.
	if drainingCount < 1 && inFlightAtDrain < 1 {
		t.Errorf("readiness withdrawal recorded no work in hand "+
			"(consumer_draining_peak=%d in_flight_at_withdrawal=%d): the instance "+
			"withdrew while idle", drainingCount, inFlightAtDrain)
	}

	exit := strings.TrimSpace(e.LoadState(t, "renamer2-exit-code"))
	if exit != "0" {
		t.Errorf("renamer-2 exited with code %q after SIGTERM, expected 0", exit)
	}
	stopSecs := strings.TrimSpace(e.LoadState(t, "renamer2-stop-seconds"))
	// Bounded, not timed. The orchestrator stops this instance with a 90s
	// grace while it holds a delivery, so what has to be true is that it exits
	// inside that budget without being killed -- not that it takes any
	// particular number of seconds. Asserting a duration would turn an
	// injected hold into a product requirement.
	if d, derr := time.ParseDuration(stopSecs + "s"); derr == nil && d >= 90*time.Second {
		t.Errorf("renamer-2 took %s to terminate, at or beyond its grace period: exit is not bounded", d)
	}

	report := fmt.Sprintf(
		"registered_during_load=%d all_reached_durable_outcome=true stranded_in_processing=%d\n"+
			"injected_holds_entered=%d\n"+
			"in_flight_at_readiness_withdrawal=%d consumer_draining_peak=%d deliveries_settled_during_shutdown=%d\n"+
			"consumer_draining_logged=%t shutdown_complete=%t shutdown_timeout=%t\n"+
			"exit_code=%s stop_duration_seconds=%s\n",
		registered, snap.ByState[jobs.StateProcessing], faultPaused,
		inFlightAtDrain, drainingCount, settledAfter,
		sawDraining, sawComplete, sawTimeout, exit, stopSecs)
	t.Logf("%s", report)
	e.WriteEvidence(t, "f5-drain-under-load.txt", []byte(report))
}

// TestF5ReadinessStaysWithdrawnThroughTheDrain checks the readiness
// observations the orchestrator sampled across the termination.
//
// A concurrent probe pass used to be able to overwrite the shutdown
// withdrawal, so an observer draining the instance could see it advertise
// itself as available again. The withdrawal is now latched off for the rest of
// the process lifetime.
//
// What this can and cannot establish, stated plainly: the drain itself is very
// short — the handlers are a single durable write each, so the window between
// the withdrawal and the last in-flight delivery settling is on the order of
// milliseconds, far below any external sampling interval. External sampling
// therefore cannot be expected to catch a not-ready-but-still-serving sample.
// What it can establish, and what is asserted here, is that no observation
// reports the instance ready at or after the moment shutdown began. The
// ordering inside the process is established separately from its own log, and
// the latch itself is covered by a unit test on the health server.
func TestF5ReadinessStaysWithdrawnThroughTheDrain(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseDrained)

	raw, ok := e.OptionalState("drain-readiness-observations")
	if !ok {
		t.Fatalf("the orchestrator recorded no readiness observations for the drain; " +
			"FN-F005 requires readiness to be observed during the drain, not inferred afterwards")
	}

	type observation struct {
		At       string `json:"at"`
		Target   string `json:"target"`
		Status   int    `json:"http_status"`
		Ready    bool   `json:"ready"`
		Draining bool   `json:"draining"`
		Error    string `json:"error"`
	}

	var observations []observation
	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var o observation
		if err := json.Unmarshal([]byte(line), &o); err != nil {
			t.Errorf("unparseable readiness observation: %s", line)
			continue
		}
		observations = append(observations, o)
	}
	if len(observations) < 5 {
		t.Fatalf("only %d readiness observation(s) were recorded across the termination; too few to establish anything",
			len(observations))
	}
	sort.SliceStable(observations, func(i, j int) bool { return observations[i].At < observations[j].At })

	// The instant shutdown began, taken from the process's own log.
	since := e.Since(t, "drain")
	shutdownAt := time.Time{}
	for _, r := range recordsSince(t, e, "renamer-2", since) {
		if r.String("event") == "shutdown_started" {
			if ts, perr := time.Parse(time.RFC3339Nano, r.String("time")); perr == nil {
				shutdownAt = ts
				break
			}
		}
	}
	if shutdownAt.IsZero() {
		t.Fatalf("renamer-2 never logged shutdown_started within the drain interval, so there is no instant to compare against")
	}

	var (
		readyBefore  int
		readyAfter   []string
		notReady     int
		unreachable  int
		lastSampleAt string
	)
	for _, o := range observations {
		lastSampleAt = o.At
		ts, perr := time.Parse(time.RFC3339Nano, o.At)
		if perr != nil {
			t.Errorf("observation has an unparseable timestamp: %s", o.At)
			continue
		}
		switch {
		case o.Error != "" || o.Status == 0:
			unreachable++
		case o.Ready:
			if ts.Before(shutdownAt) {
				readyBefore++
			} else {
				// Ready at or after shutdown began: the withdrawal was
				// contradicted, which is precisely the defect.
				readyAfter = append(readyAfter, o.At)
			}
		default:
			notReady++
		}
	}

	if readyBefore < 1 {
		t.Errorf("no observation showed the instance ready before shutdown, so the sampler was not watching a serving instance")
	}
	if len(readyAfter) > 0 {
		t.Errorf("the instance reported itself ready at or after shutdown began (%s), at %v: "+
			"the withdrawal was contradicted", shutdownAt.Format(time.RFC3339Nano), readyAfter)
	}
	// The sampler must have outlasted the termination, or it cannot have
	// observed anything about it.
	if lastTs, perr := time.Parse(time.RFC3339Nano, lastSampleAt); perr == nil && lastTs.Before(shutdownAt) {
		t.Errorf("sampling stopped at %s, before shutdown began at %s: the termination was never observed",
			lastSampleAt, shutdownAt.Format(time.RFC3339Nano))
	}
	if notReady+unreachable < 1 {
		t.Errorf("no observation showed the instance not ready or gone after shutdown began")
	}

	report := fmt.Sprintf(
		"observations=%d shutdown_started=%s\n"+
			"ready_before_shutdown=%d ready_at_or_after_shutdown=%d not_ready=%d unreachable_or_gone=%d\n"+
			"last_sample=%s\n"+
			"note: the drain window is milliseconds (each handler is one durable write), so an external\n"+
			"      sampler cannot resolve a not-ready-but-still-serving sample; the assertion is that the\n"+
			"      withdrawal is never contradicted, with in-process ordering established from the log.\n",
		len(observations), shutdownAt.Format(time.RFC3339Nano),
		readyBefore, len(readyAfter), notReady, unreachable, lastSampleAt)
	t.Logf("%s", report)
	e.WriteEvidence(t, "f5-drain-readiness-observations.jsonl", []byte(raw+"\n"))
	e.WriteEvidence(t, "f5-drain-readiness-summary.txt", []byte(report))
}
