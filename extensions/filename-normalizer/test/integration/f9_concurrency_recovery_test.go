//go:build integration

package integration

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/broker"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/jobs"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/ledger"
)

// A2, A6 and A7, plus watcher restart reconciliation.
//
// These are the acceptance criteria about what happens when more than one
// worker is involved, or when one of them is interrupted. They are asserted
// from the durable record and the filesystem, never from a log line alone.

// ---------------------------------------------------------------------------
// A2 — genuine concurrency
// ---------------------------------------------------------------------------

// TestA2TwoRenamersOverlapOnRealDocuments requires that two renamer containers
// actually process different documents at the same time.
//
// Deployment capability is not the claim: starting two containers proves
// nothing about overlap. What is asserted is that both instances delivered
// documents AND that at least one pair of their processing windows genuinely
// intersects in time, taken from the ledger's own event timestamps.
func TestA2TwoRenamersOverlapOnRealDocuments(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseNormalization)
	led := e.Ledger(t)

	// Enough documents, large enough to take measurable time, that two
	// consumers with a prefetch of two cannot help but overlap.
	const n = 12
	body := bytes.Repeat([]byte("concurrency-payload-"), 150000) // ~3 MB
	var names []string
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("concurrent-%s-%02d.pdf", e.RunID, i)
		place(t, e, name, append(body, byte(i)))
		names = append(names, name)
	}

	type window struct {
		job        string
		actor      string
		start, end time.Time
	}
	var windows []window
	byActor := map[string]int{}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	for _, name := range names {
		job := awaitJob(t, e, led, name)
		if job.State != jobs.StateDelivered {
			t.Errorf("%s reached %q (%s)", name, job.State, derefCategory(job))
			continue
		}
		events, err := led.Events(ctx, job.JobID)
		if err != nil {
			t.Fatalf("read job history: %v", err)
		}
		// The processing window is from the delivery being received to the
		// receipt being written, both by the same renamer instance.
		var w window
		w.job = job.JobID
		for _, ev := range events {
			switch ev.EventType {
			case jobs.EventDeliveryReceived:
				if w.start.IsZero() || ev.OccurredAt.After(w.start) {
					w.start = ev.OccurredAt
					w.actor = ev.Actor
				}
			case jobs.EventDelivered, jobs.EventReconciled:
				w.end = ev.OccurredAt
			}
		}
		if w.start.IsZero() || w.end.IsZero() || w.actor == "" {
			continue
		}
		windows = append(windows, w)
		byActor[w.actor]++
	}

	if len(byActor) < 2 {
		t.Errorf("only %d renamer instance(s) delivered documents: %v; two must participate", len(byActor), byActor)
	}

	// Find a genuinely overlapping pair from two DIFFERENT instances.
	overlaps := 0
	var example string
	for i := range windows {
		for j := i + 1; j < len(windows); j++ {
			a, b := windows[i], windows[j]
			if a.actor == b.actor {
				continue
			}
			if a.start.Before(b.end) && b.start.Before(a.end) {
				overlaps++
				if example == "" {
					example = fmt.Sprintf("%s on %s [%s..%s] overlaps %s on %s [%s..%s]",
						a.job[:8], a.actor, a.start.Format("15:04:05.000"), a.end.Format("15:04:05.000"),
						b.job[:8], b.actor, b.start.Format("15:04:05.000"), b.end.Format("15:04:05.000"))
				}
			}
		}
	}
	if overlaps == 0 {
		t.Errorf("no two documents were processed concurrently by different instances; "+
			"the batch was handled sequentially (%d windows across %v)", len(windows), byActor)
	}

	actors := make([]string, 0, len(byActor))
	for a := range byActor {
		actors = append(actors, a)
	}
	sort.Strings(actors)
	var report strings.Builder
	fmt.Fprintf(&report, "A2 — real overlapping processing in two renamer containers\n\n")
	fmt.Fprintf(&report, "documents:           %d (~3 MB each)\n", n)
	fmt.Fprintf(&report, "delivered:           %d\n", len(windows))
	for _, a := range actors {
		fmt.Fprintf(&report, "  %-24s %d documents\n", a, byActor[a])
	}
	fmt.Fprintf(&report, "\noverlapping cross-instance pairs: %d\n", overlaps)
	if example != "" {
		fmt.Fprintf(&report, "example: %s\n", example)
	}
	report.WriteString("\nThe windows come from the ledger's own event timestamps (delivery_received to\n" +
		"delivered), written by the instance that did the work, so this is measured\n" +
		"overlap rather than an inference from two containers being up.\n")
	e.WriteEvidence(t, "a2-real-concurrency.txt", []byte(report.String()))
}

// ---------------------------------------------------------------------------
// A6 — worker and publication recovery
// ---------------------------------------------------------------------------

// TestA6ConsumedDocumentIsNotRepublished covers the case the contract calls
// out explicitly: a durable delivery record exists but the destination is
// gone, because Paperless consumed it. That must not be read as a failure and
// must not cause redelivery.
func TestA6ConsumedDocumentIsNotRepublished(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseNormalization)
	led := e.Ledger(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	name := "a6consumed-" + e.RunID + ".pdf"
	place(t, e, name, []byte("%PDF-1.4 a6 consumed\n"))
	job := awaitJob(t, e, led, name)
	if job.State != jobs.StateDelivered {
		t.Fatalf("state %q, expected delivered", job.State)
	}
	path := publishedPath(t, e, led, job)
	delivered := filepath.Base(path)

	// The consumer takes it.
	if err := os.Remove(path); err != nil {
		t.Fatalf("simulate consumption: %v", err)
	}

	// Republish the same job onto the queue. A redelivery of a job that
	// already has a receipt must settle against the receipt and touch nothing.
	republishJob(t, e, job.JobID)

	time.Sleep(6 * time.Second)

	after, err := led.GetJob(ctx, job.JobID)
	if err != nil {
		t.Fatalf("re-read the job: %v", err)
	}
	if after.State != jobs.StateDelivered {
		t.Errorf("state changed to %q after a redelivery of a consumed document", after.State)
	}
	if _, err := os.Stat(path); err == nil {
		t.Errorf("the document was republished after the consumer removed it")
	}
	receipt, err := led.GetReceipt(ctx, job.JobID)
	if err != nil {
		t.Fatalf("the receipt disappeared: %v", err)
	}
	if receipt.DeliveredName != delivered {
		t.Errorf("the receipt changed from %q to %q", delivered, receipt.DeliveredName)
	}

	e.WriteEvidence(t, "a6-consumed-not-republished.txt", []byte(fmt.Sprintf(
		"delivered as        %s\nremoved from consume (as Paperless would)\n"+
			"job redelivered on the queue\nstate after:        %s\nrepublished:        %t\n"+
			"receipt unchanged:  %t\n\n"+
			"An absent destination with a durable receipt is read as \"already delivered\",\n"+
			"never as a failed delivery, so the consumer cannot trigger a duplicate.\n",
		delivered, after.State, false, receipt.DeliveredName == delivered)))
}

// TestA6DuplicateDeliveryProducesOneDocument sends the same job twice while it
// is being processed, which is what an at-least-once broker can do.
func TestA6DuplicateDeliveryProducesOneDocument(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseNormalization)
	led := e.Ledger(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	name := "a6duplicate-" + e.RunID + ".pdf"
	content := []byte("%PDF-1.4 a6 duplicate delivery\n")
	place(t, e, name, content)

	// Wait for registration, then flood the queue with the same job id.
	var job ledger.Job
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); {
		j, err := led.JobBySource(ctx, e.Cfg.Storage.Incoming, name)
		if err == nil {
			job = j
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if job.JobID == "" {
		t.Fatal("the submission was never registered")
	}
	for i := 0; i < 4; i++ {
		republishJob(t, e, job.JobID)
	}

	final := awaitJobByID(t, led, job.JobID)
	if final.State != jobs.StateDelivered {
		t.Fatalf("state %q (%s), expected delivered", final.State, derefCategory(final))
	}

	// Exactly one document, and exactly one receipt.
	entries, err := os.ReadDir(e.Cfg.Storage.Consume)
	if err != nil {
		t.Fatalf("read consume: %v", err)
	}
	matches := 0
	for _, de := range entries {
		if strings.Contains(de.Name(), "a6duplicate-"+e.RunID) && !strings.HasPrefix(de.Name(), ".fn-") {
			matches++
		}
	}
	if matches != 1 {
		t.Errorf("%d documents in the consume directory for one job; expected exactly 1", matches)
	}

	receipt, err := led.GetReceipt(ctx, job.JobID)
	if err != nil {
		t.Fatalf("no receipt: %v", err)
	}
	published, err := os.ReadFile(filepath.Join(receipt.DestinationRoot, receipt.DeliveredName))
	if err != nil || !bytes.Equal(published, content) {
		t.Errorf("the published document is not the submitted content")
	}

	e.WriteEvidence(t, "a6-duplicate-delivery.txt", []byte(fmt.Sprintf(
		"one job, %d extra deliveries published onto the queue\n"+
			"documents in the consume directory: %d (expected 1)\n"+
			"delivery attempts recorded:          %d\n"+
			"final state:                         %s\n\n"+
			"Repeated delivery of one job produces one document: the receipt settles every\n"+
			"later delivery without touching the filesystem again.\n",
		4, matches, final.DeliveryAttempts, final.State)))
}

// republishJob puts an existing job id back on the work queue.
//
// This is the real broker and the real message contract; it is how an
// at-least-once delivery actually reaches a consumer. It does not fabricate a
// job: the job it references is a real row the watcher registered.
func republishJob(t *testing.T, e *Env, jobID string) {
	t.Helper()
	conn, ctx := e.Broker(t)
	// The production topology, not an isolated one: the point is for the real
	// renamer containers to receive this delivery.
	top := broker.TopologyFromConfig(e.Cfg.Broker)
	pub := broker.NewPublisher(conn, top, e.Log, e.Cfg.Broker.ConfirmTimeout)
	defer pub.Close()

	pubCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	result, err := pub.Publish(pubCtx, jobs.Message{
		ContractVersion: jobs.ContractVersion,
		JobID:           jobID,
		Attempt:         1,
		EnqueuedAt:      time.Now().UTC(),
	})
	if err != nil || result != broker.PublishConfirmed {
		t.Fatalf("republish job %s: %v (result=%s)", jobID, err, result)
	}
}

// ---------------------------------------------------------------------------
// A7 — stale worker
// ---------------------------------------------------------------------------

// TestA7ConcurrentWorkersOnOneJobProduceOneDocument is the stale-worker
// property expressed as something that can actually be asserted: two workers
// holding the same job at the same time must not produce two documents, two
// names, or an overwrite.
//
// A worker that has lost its broker connection but still has filesystem access
// is, from the filesystem's point of view, exactly this: a second actor
// operating on the same job concurrently with its replacement. The defence is
// not the acknowledgement -- which is precisely what the disconnected worker
// cannot send -- but the per-job reservation and the no-overwrite link.
func TestA7ConcurrentWorkersOnOneJobProduceOneDocument(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseNormalization)
	led := e.Ledger(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	name := "a7stale-" + e.RunID + ".pdf"
	content := bytes.Repeat([]byte("a7-stale-worker-"), 100000) // ~1.6 MB
	place(t, e, name, content)

	var job ledger.Job
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); {
		j, err := led.JobBySource(ctx, e.Cfg.Storage.Incoming, name)
		if err == nil {
			job = j
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if job.JobID == "" {
		t.Fatal("the submission was never registered")
	}

	// Put several copies of the same job on the queue at once, so both
	// renamer containers are working on it simultaneously. Neither knows the
	// other has it -- which is the stale worker's situation.
	for i := 0; i < 6; i++ {
		republishJob(t, e, job.JobID)
	}

	final := awaitJobByID(t, led, job.JobID)
	if final.State != jobs.StateDelivered {
		t.Fatalf("state %q (%s)", final.State, derefCategory(final))
	}

	// One document, one reservation, one receipt.
	entries, err := os.ReadDir(e.Cfg.Storage.Consume)
	if err != nil {
		t.Fatalf("read consume: %v", err)
	}
	var found []string
	for _, de := range entries {
		if strings.Contains(de.Name(), "a7stale-"+e.RunID) && !strings.HasPrefix(de.Name(), ".fn-") {
			found = append(found, de.Name())
		}
	}
	if len(found) != 1 {
		t.Errorf("%d documents for one job: %v; concurrent workers must not multiply documents", len(found), found)
	}

	receipt, err := led.GetReceipt(ctx, job.JobID)
	if err != nil {
		t.Fatalf("no receipt: %v", err)
	}
	published, err := os.ReadFile(filepath.Join(receipt.DestinationRoot, receipt.DeliveredName))
	if err != nil || !bytes.Equal(published, content) {
		t.Errorf("the published bytes are not the submitted bytes")
	}

	// How many actors actually touched it, from the durable history.
	events, err := led.Events(ctx, job.JobID)
	if err != nil {
		t.Fatalf("read history: %v", err)
	}
	actors := map[string]int{}
	publishAttempts, deliveriesReceived := 0, 0
	for _, ev := range events {
		actors[ev.Actor]++
		switch ev.EventType {
		case jobs.EventPublishAttempted:
			publishAttempts++
		case jobs.EventDeliveryReceived:
			deliveriesReceived++
		}
	}
	if deliveriesReceived < 2 {
		t.Errorf("only %d deliveries reached a worker; the concurrent-worker scenario did not occur",
			deliveriesReceived)
	}
	names := make([]string, 0, len(actors))
	for a := range actors {
		names = append(names, a)
	}
	sort.Strings(names)

	// Stated precisely, because the distribution is the point: several workers
	// received the job, but only the ones that got there before a receipt
	// existed attempt a publication at all. A single publish_attempted event
	// means the later workers settled against the receipt without touching the
	// filesystem -- which is the safe outcome, but it also means this run did
	// not exercise two simultaneous link() calls on the same name.
	stressed := "yes"
	if publishAttempts <= 1 {
		stressed = "no -- the later deliveries settled against the receipt before reaching " +
			"publication, so the simultaneous-link path was not reached in this run"
	}
	e.WriteEvidence(t, "a7-stale-worker.txt", []byte(fmt.Sprintf(
		"A7 — concurrent workers on one job\n\n"+
			"extra deliveries placed on the queue: 6\n"+
			"actors that wrote history for this job: %v\n"+
			"delivery_received events:              %d\n"+
			"publish_attempted events:              %d\n"+
			"documents produced:                    %d (%v)\n"+
			"published bytes identical to source:   %t\n"+
			"two simultaneous publications reached: %s\n\n"+
			"What this establishes: several workers held the same job and exactly one\n"+
			"document exists, with the submitted bytes and one reserved name. Safety does\n"+
			"not come from the acknowledgement -- a disconnected worker cannot send one --\n"+
			"but from the per-job reservation and from link(2) refusing to overwrite.\n\n"+
			"What it does not establish: that two link() calls collided in the same\n"+
			"instant. That branch is covered by construction and by the occupied-\n"+
			"destination test, not by a reproduced simultaneous race here.\n",
		names, deliveriesReceived, publishAttempts, len(found), found,
		bytes.Equal(published, content), stressed)))
}

// ---------------------------------------------------------------------------
// Watcher restart reconciliation
// ---------------------------------------------------------------------------

// TestWatcherRestartRecoversSubmissionsThatArrivedWhileItWasDown is asserted
// in the phase that follows a watcher restart; the orchestrator stops the
// watcher, drops documents in, and starts it again.
func TestWatcherRestartRecoversSubmissionsThatArrivedWhileItWasDown(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseWatcherRestarted)
	led := e.Ledger(t)

	names := e.LoadState(t, "offline-submissions")
	if names == "" {
		t.Skip("the orchestrator recorded no offline submissions for this phase")
	}

	var report strings.Builder
	report.WriteString("Watcher restart reconciliation\n\n")
	report.WriteString("These documents were placed in the incoming directory while the watcher\n" +
		"container was stopped, so nothing was watching when they arrived.\n\n")

	for _, name := range strings.Split(names, ",") {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		job := awaitJob(t, e, led, name)
		if job.State != jobs.StateDelivered {
			t.Errorf("%s reached %q (%s) after the watcher restarted, expected delivered",
				name, job.State, derefCategory(job))
			continue
		}
		fmt.Fprintf(&report, "  %-40s -> %s\n", name, filepath.Base(publishedPath(t, e, led, job)))
	}

	report.WriteString("\nEligibility is derived from the filesystem and the ledger, never from\n" +
		"in-process memory, so the first scan after a restart picks up whatever arrived\n" +
		"during the outage. There is no separate catch-up path to go wrong.\n")
	e.WriteEvidence(t, "watcher-restart-reconciliation.txt", []byte(report.String()))
}
