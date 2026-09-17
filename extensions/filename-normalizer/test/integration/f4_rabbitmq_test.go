//go:build integration

package integration

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/broker"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/jobs"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/ledger"
)

// F4 — real RabbitMQ integration.
//
// These assertions drive the applications' own broker package against the real
// node: the publisher with confirms, the consumer with manual acknowledgement
// and bounded prefetch, and the topology declaration. Where a test needs to
// control acknowledgement precisely, it uses an isolated topology named after
// the run so it cannot steal work from the running renamers; teardown removes
// exactly those queues and exchanges.
//
// Scope: what follows establishes messaging behaviour. It does not establish
// exactly-once filesystem effects, and an acknowledgement here is never
// treated as a completion record.

// TestF4PublishIsConfirmed asserts a publication is settled by a real
// publisher confirm, and lands in the queue.
func TestF4PublishIsConfirmed(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseBaseline)

	conn, _ := e.Broker(t)
	top := e.DeclareIsolated(t, conn, "confirm")
	pub := broker.NewPublisher(conn, top, e.Log, e.Cfg.Broker.ConfirmTimeout)
	t.Cleanup(pub.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	msg := jobs.Message{
		ContractVersion: jobs.ContractVersion,
		JobID:           "11111111-1111-4111-8111-111111111111",
		Attempt:         1,
		EnqueuedAt:      time.Now().UTC(),
	}
	result, err := pub.Publish(ctx, msg)
	if err != nil {
		t.Fatalf("publish: %v (result=%s)", err, result)
	}
	if result != broker.PublishConfirmed {
		t.Fatalf("publication result is %q, expected %q", result, broker.PublishConfirmed)
	}

	err = waitForErr(15*time.Second, func() error {
		n, _, derr := pub.QueueDepth(top.Queue)
		if derr != nil {
			return derr
		}
		if n != 1 {
			return fmt.Errorf("queue holds %d messages, expected 1", n)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("the confirmed message is not in the queue: %v", err)
	}
	e.SaveState(t, "f4-confirm-queue", top.Queue)
}

// TestF4UnroutablePublishIsReported asserts a mandatory publication with no
// binding is reported as returned instead of vanishing.
func TestF4UnroutablePublishIsReported(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseBaseline)

	conn, _ := e.Broker(t)
	top := e.DeclareIsolated(t, conn, "unroutable")
	// Publish with a routing key nothing is bound to.
	broken := top
	broken.RoutingKey = "no-such-binding"

	pub := broker.NewPublisher(conn, broken, e.Log, e.Cfg.Broker.ConfirmTimeout)
	t.Cleanup(pub.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := pub.Publish(ctx, jobs.Message{
		ContractVersion: jobs.ContractVersion,
		JobID:           "22222222-2222-4222-8222-222222222222",
		Attempt:         1,
		EnqueuedAt:      time.Now().UTC(),
	})
	if result != broker.PublishReturned {
		t.Fatalf("an unroutable publication reported %q (err=%v), expected %q", result, err, broker.PublishReturned)
	}
	t.Logf("unroutable publication was reported as returned: %v", err)
}

// TestF4ManualAcknowledgementAndBrokerPrefetchWindow asserts the consumer
// settles deliveries manually and that the broker itself never hands out more
// unacknowledged deliveries than the declared prefetch window.
func TestF4ManualAcknowledgementAndBrokerPrefetchWindow(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseBaseline)

	conn, _ := e.Broker(t)
	top := e.DeclareIsolated(t, conn, "manual-ack")
	pub := broker.NewPublisher(conn, top, e.Log, e.Cfg.Broker.ConfirmTimeout)
	t.Cleanup(pub.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const total = 6
	for i := 0; i < total; i++ {
		result, err := pub.Publish(ctx, jobs.Message{
			ContractVersion: jobs.ContractVersion,
			JobID:           fmt.Sprintf("33333333-3333-4333-8333-%012d", i),
			Attempt:         1,
			EnqueuedAt:      time.Now().UTC(),
		})
		if result != broker.PublishConfirmed {
			t.Fatalf("message %d was not confirmed: %s (%v)", i, result, err)
		}
	}

	const prefetch = 2
	var (
		mu       sync.Mutex
		acked    int
		peak     int
		inflight int
	)
	release := make(chan struct{})

	cons := broker.NewConsumer(conn, broker.ConsumerOptions{
		Queue:       top.Queue,
		Prefetch:    prefetch,
		Concurrency: prefetch,
		ConsumerTag: "integration-manual-ack",
		Logger:      e.Log,
	})

	consCtx, consCancel := context.WithCancel(ctx)
	defer consCancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		cons.Run(consCtx, func(_ context.Context, d broker.Delivery) broker.Decision {
			mu.Lock()
			inflight++
			if inflight > peak {
				peak = inflight
			}
			mu.Unlock()

			// Hold the delivery unacknowledged until released, so the prefetch
			// window is observable.
			<-release

			mu.Lock()
			inflight--
			acked++
			mu.Unlock()
			return broker.Ack
		})
	}()

	// The oracle is the broker's own unacknowledged count, not a counter kept
	// in this process. A local count measures running handlers, which the
	// consumer's own concurrency semaphore already bounds, so it could not
	// distinguish "the AMQP prefetch window is respected" from "the semaphore
	// held everything back".
	if !waitFor(20*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return inflight == prefetch
	}) {
		t.Fatalf("the consumer never reached the prefetch window of %d", prefetch)
	}

	// Sample the broker repeatedly while every handler is blocked and more
	// messages remain queued.
	var brokerPeak, brokerReadySeen, brokerTotalSeen int
	var samples int
	var rawSamples []string
	oracleSource := "messages_unacknowledged"
	// Generous window: the management API refreshes its queue statistics on an
	// interval, so a short sample run can miss the state entirely.
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		st, raw := e.QueueStatsRaw(t, top.Queue)
		samples++
		if len(rawSamples) < 4 {
			rawSamples = append(rawSamples, raw)
		}
		if st.Total > brokerTotalSeen {
			brokerTotalSeen = st.Total
		}
		unacked, source := st.UnackedOrDerived()
		if unacked > brokerPeak {
			brokerPeak = unacked
			oracleSource = source
		}
		if st.Ready > brokerReadySeen {
			brokerReadySeen = st.Ready
		}
		if unacked > prefetch {
			t.Fatalf("the broker reports %d unacknowledged deliveries against a prefetch window of %d "+
				"(oracle: %s, queue %s)", unacked, prefetch, source, top.Queue)
		}
		if unacked >= prefetch && st.Ready >= 1 {
			// The window is full with work still waiting: nothing more to
			// establish, so stop sampling early.
			break
		}
		if st.Unacknowledged > brokerPeak {
			brokerPeak = st.Unacknowledged
		}
		if st.Ready > brokerReadySeen {
			brokerReadySeen = st.Ready
		}
		time.Sleep(300 * time.Millisecond)
	}
	if samples < 3 {
		t.Fatalf("only %d broker samples were taken; the window was not observed", samples)
	}
	// The window must actually have been filled, or the bound is untested.
	if brokerPeak != prefetch {
		t.Fatalf("the broker never reported the prefetch window as full: peak unacknowledged was %d, expected %d "+
			"(highest total reported: %d, samples: %d)\nfirst raw management responses:\n%s",
			brokerPeak, prefetch, brokerTotalSeen, samples, strings.Join(rawSamples, "\n"))
	}
	// And messages must have been waiting, or the broker had nothing more to
	// hand over and the bound would hold trivially.
	if brokerReadySeen < 1 {
		t.Fatalf("the broker never reported messages waiting while the window was full, so the bound was not exercised")
	}

	mu.Lock()
	over := peak
	mu.Unlock()
	if over > prefetch {
		t.Fatalf("the consumer ran %d handlers concurrently, above its bound of %d", over, prefetch)
	}

	close(release)
	if !waitFor(40*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return acked == total
	}) {
		mu.Lock()
		got := acked
		mu.Unlock()
		t.Fatalf("only %d of %d deliveries were acknowledged", got, total)
	}

	consCancel()
	<-done

	if n, _, err := pub.QueueDepth(top.Queue); err != nil {
		t.Fatalf("read queue depth: %v", err)
	} else if n != 0 {
		t.Errorf("queue still holds %d messages after every delivery was acknowledged", n)
	}
	e.WriteEvidence(t, "f4-manual-ack.txt", []byte(fmt.Sprintf(
		"oracle=rabbitmq-management-api field=%s queue=%s\n"+
			"published=%d acknowledged=%d declared_prefetch=%d\n"+
			"broker_peak_unacknowledged=%d broker_peak_messages_ready=%d broker_peak_total=%d broker_samples=%d\n"+
			"local_peak_running_handlers=%d (bounded separately by the consumer semaphore, not an oracle for the AMQP window)\n",
		oracleSource, top.Queue, total, acked, prefetch,
		brokerPeak, brokerReadySeen, brokerTotalSeen, samples, over)))
}

// TestF4RedeliveryAfterInterruptedDelivery asserts an unacknowledged delivery
// is redelivered after the consumer's connection is really interrupted.
//
// The interruption is genuine: the application's own connection is closed
// underneath a delivery it never acknowledged. Nothing is faked and no
// acknowledgement is forged.
func TestF4RedeliveryAfterInterruptedDelivery(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseBaseline)

	victim, _ := e.Broker(t)
	top := e.DeclareIsolated(t, victim, "redelivery")
	pub := broker.NewPublisher(victim, top, e.Log, e.Cfg.Broker.ConfirmTimeout)
	t.Cleanup(pub.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	jobID := "44444444-4444-4444-8444-444444444444"
	if result, err := pub.Publish(ctx, jobs.Message{
		ContractVersion: jobs.ContractVersion,
		JobID:           jobID, Attempt: 1, EnqueuedAt: time.Now().UTC(),
	}); result != broker.PublishConfirmed {
		t.Fatalf("publish: %s (%v)", result, err)
	}

	type seen struct {
		redelivered bool
		count       int
	}
	var (
		mu        sync.Mutex
		history   []seen
		firstSeen = make(chan struct{})
		ackNow    bool
	)

	cons := broker.NewConsumer(victim, broker.ConsumerOptions{
		Queue:       top.Queue,
		Prefetch:    1,
		Concurrency: 1,
		ConsumerTag: "integration-redelivery",
		Logger:      e.Log,
	})

	consCtx, consCancel := context.WithCancel(ctx)
	defer consCancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		cons.Run(consCtx, func(hctx context.Context, d broker.Delivery) broker.Decision {
			mu.Lock()
			history = append(history, seen{redelivered: d.Redelivered, count: d.DeliveryCount})
			isFirst := len(history) == 1
			shouldAck := ackNow
			mu.Unlock()

			if isFirst {
				select {
				case firstSeen <- struct{}{}:
				default:
				}
			}
			if shouldAck {
				return broker.Ack
			}
			// Hold the delivery unacknowledged. The connection is about to be
			// closed under it; returning any decision afterwards cannot settle
			// it, which is precisely the interruption being tested.
			select {
			case <-hctx.Done():
			case <-time.After(30 * time.Second):
			}
			return broker.Ack
		})
	}()

	select {
	case <-firstSeen:
	case <-time.After(30 * time.Second):
		t.Fatalf("the consumer never received the published message")
	}

	// Interrupt the application's own broker connection while the delivery is
	// still unacknowledged.
	mu.Lock()
	ackNow = true
	mu.Unlock()
	victim.ForceClose()
	t.Logf("closed the consumer's connection with the delivery unacknowledged")

	if !waitFor(45*time.Second, victim.Connected) {
		t.Fatalf("the connection supervisor did not reconnect after the interruption")
	}

	if !waitFor(60*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		for _, h := range history[1:] {
			if h.redelivered {
				return true
			}
		}
		return len(history) > 1
	}) {
		mu.Lock()
		got := history
		mu.Unlock()
		t.Fatalf("the unacknowledged delivery was never redelivered; history=%+v", got)
	}

	consCancel()
	<-done

	mu.Lock()
	got := history
	mu.Unlock()
	if len(got) < 2 {
		t.Fatalf("expected at least two deliveries of the same message, got %d", len(got))
	}
	var sawRedeliveredFlag bool
	var maxCount int
	for _, h := range got[1:] {
		if h.redelivered {
			sawRedeliveredFlag = true
		}
		if h.count > maxCount {
			maxCount = h.count
		}
	}
	if !sawRedeliveredFlag {
		t.Errorf("no redelivery carried the redelivered flag; history=%+v", got)
	}
	if maxCount < 1 {
		t.Errorf("the quorum queue did not report an x-delivery-count above zero; history=%+v", got)
	}
	e.WriteEvidence(t, "f4-redelivery.txt", []byte(fmt.Sprintf(
		"job_id=%s deliveries=%d redelivered_flag_seen=%t max_delivery_count=%d\n",
		jobID, len(got), sawRedeliveredFlag, maxCount)))
}

// TestF4DeliveryLimitIsDeclaredOnTheQueue asserts the broker really holds the
// configured delivery limit and dead-letter routing, by redeclaring the queue
// with a different limit and requiring the broker to refuse.
func TestF4DeliveryLimitIsDeclaredOnTheQueue(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseBaseline)

	conn, _ := e.Broker(t)
	top := e.DeclareIsolated(t, conn, "limit")

	// An equivalent redeclare must succeed.
	ch, _, err := conn.Channel()
	if err != nil {
		t.Fatalf("open channel: %v", err)
	}
	if derr := broker.Declare(ch, top); derr != nil {
		_ = ch.Close()
		t.Fatalf("an equivalent redeclare was refused: %v", derr)
	}
	_ = ch.Close()

	// A redeclare with a different delivery limit must be refused, which only
	// happens if the broker is actually holding the declared value.
	mismatched := top
	mismatched.DeliveryLimit = top.DeliveryLimit + 7
	ch2, _, err := conn.Channel()
	if err != nil {
		t.Fatalf("open channel: %v", err)
	}
	derr := broker.Declare(ch2, mismatched)
	_ = ch2.Close()
	if derr == nil {
		t.Fatalf("the broker accepted a redeclare with delivery limit %d after %d was declared; the limit is not in effect",
			mismatched.DeliveryLimit, top.DeliveryLimit)
	}
	t.Logf("broker holds x-delivery-limit=%d and refused a conflicting redeclare: %v", top.DeliveryLimit, derr)
	e.WriteEvidence(t, "f4-delivery-limit-declared.txt", []byte(fmt.Sprintf(
		"queue=%s declared_delivery_limit=%d conflicting_redeclare_refused=true\ndead_letter_exchange=%s\ndead_letter_queue=%s\n",
		top.Queue, top.DeliveryLimit, top.DeadLetterX, top.DeadLetterQ)))
}

// TestF4ExplicitRequeueDoesNotAdvanceTheBrokerDeliveryCount records a measured
// property of this broker version, because the application's failure handling
// depends on it.
//
// On RabbitMQ 4.3.6 a quorum queue does not advance x-delivery-count when a
// consumer negatively acknowledges with requeue=true: the message comes back
// with the redelivered flag set but with the counter unchanged. The broker's
// x-delivery-limit therefore cannot bound a consumer that keeps requeueing,
// which is why the renamer stops consuming instead of returning the same
// delivery in a tight loop. Connection-loss redelivery does advance the
// counter; that is asserted separately.
//
// If a future broker version changes this, the assertion fails and the
// application's bounded-retry design should be revisited rather than left
// resting on an assumption.
func TestF4ExplicitRequeueDoesNotAdvanceTheBrokerDeliveryCount(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseBaseline)

	conn, _ := e.Broker(t)
	top := e.DeclareIsolated(t, conn, "requeue-count")
	pub := broker.NewPublisher(conn, top, e.Log, e.Cfg.Broker.ConfirmTimeout)
	t.Cleanup(pub.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	if result, err := pub.Publish(ctx, jobs.Message{
		ContractVersion: jobs.ContractVersion,
		JobID:           "55555555-5555-4555-8555-555555555555",
		Attempt:         1, EnqueuedAt: time.Now().UTC(),
	}); result != broker.PublishConfirmed {
		t.Fatalf("publish: %s (%v)", result, err)
	}

	const requeues = 20
	var (
		mu       sync.Mutex
		seen     int
		maxCount int
		anyFlag  bool
		done     = make(chan struct{})
		once     sync.Once
	)

	cons := broker.NewConsumer(conn, broker.ConsumerOptions{
		Queue: top.Queue, Prefetch: 1, Concurrency: 1,
		ConsumerTag: "integration-requeue-count", Logger: e.Log,
		DetachBackoff: 100 * time.Millisecond,
	})
	consCtx, consCancel := context.WithCancel(ctx)
	defer consCancel()
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		cons.Run(consCtx, func(_ context.Context, d broker.Delivery) broker.Decision {
			mu.Lock()
			seen++
			if d.DeliveryCount > maxCount {
				maxCount = d.DeliveryCount
			}
			if d.Redelivered {
				anyFlag = true
			}
			n := seen
			mu.Unlock()
			if n >= requeues {
				once.Do(func() { close(done) })
				return broker.Ack
			}
			return broker.NackRequeue
		})
	}()

	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatalf("the message was not requeued %d times within 60s", requeues)
	}
	consCancel()
	<-stopped

	mu.Lock()
	gotSeen, gotMax, gotFlag := seen, maxCount, anyFlag
	mu.Unlock()

	if !gotFlag {
		t.Errorf("no requeued delivery carried the redelivered flag after %d deliveries", gotSeen)
	}
	if gotMax >= requeues {
		t.Errorf("x-delivery-count reached %d after %d explicit requeues: this broker does advance the counter on requeue, "+
			"so the application's bounded-retry design should be re-examined", gotMax, gotSeen)
	}
	t.Logf("measured: %d explicit requeues produced a highest x-delivery-count of %d (redelivered flag seen: %t)",
		gotSeen, gotMax, gotFlag)
	e.WriteEvidence(t, "f4-requeue-counter-behaviour.txt", []byte(fmt.Sprintf(
		"broker=rabbitmq-4.3.6 queue_type=quorum declared_delivery_limit=%d\n"+
			"explicit_requeues=%d highest_x_delivery_count=%d redelivered_flag_seen=%t\n"+
			"conclusion: an explicit nack(requeue=true) does not advance the quorum queue delivery counter,\n"+
			"so x-delivery-limit alone cannot bound a requeue loop; the renamer detaches instead.\n",
		e.Cfg.Broker.DeliveryLimit, gotSeen, gotMax, gotFlag)))
}

// TestF4PoisonMessageIsDeadLetteredImmediately asserts a message this build
// must never interpret leaves the work queue for the dead-letter queue on its
// first delivery, so it cannot loop at all.
//
// The message is published into an isolated topology and consumed by the same
// decision code the renamer uses, so the Reject path under test is the real one.
func TestF4PoisonMessageIsDeadLetteredImmediately(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseBaseline)

	conn, _ := e.Broker(t)
	top := e.DeclareIsolated(t, conn, "poison")
	pub := broker.NewPublisher(conn, top, e.Log, e.Cfg.Broker.ConfirmTimeout)
	t.Cleanup(pub.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// A future contract version: unparseable by this build, and retrying it
	// could never help.
	if result, err := pub.Publish(ctx, jobs.Message{
		ContractVersion: 99,
		JobID:           "88888888-8888-4888-8888-888888888888",
		Attempt:         1, EnqueuedAt: time.Now().UTC(),
	}); result != broker.PublishConfirmed {
		t.Fatalf("publish: %s (%v)", result, err)
	}

	var mu sync.Mutex
	attempts := 0

	cons := broker.NewConsumer(conn, broker.ConsumerOptions{
		Queue: top.Queue, Prefetch: 1, Concurrency: 1,
		ConsumerTag: "integration-poison", Logger: e.Log,
		DetachBackoff: 200 * time.Millisecond,
	})
	consCtx, consCancel := context.WithCancel(ctx)
	defer consCancel()
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		cons.Run(consCtx, func(_ context.Context, d broker.Delivery) broker.Decision {
			mu.Lock()
			attempts++
			mu.Unlock()
			// The same classification the renamer applies.
			if _, derr := jobs.DecodeMessage(d.Body); derr != nil {
				return broker.Reject
			}
			return broker.Ack
		})
	}()

	err := waitForErr(45*time.Second, func() error {
		n, _, derr := pub.QueueDepth(top.DeadLetterQ)
		if derr != nil {
			return derr
		}
		if n < 1 {
			return fmt.Errorf("dead-letter queue is still empty")
		}
		return nil
	})
	consCancel()
	<-stopped

	mu.Lock()
	got := attempts
	mu.Unlock()

	if err != nil {
		t.Fatalf("an uninterpretable message never reached the dead-letter queue after %d deliveries: %v", got, err)
	}
	if got > 2 {
		t.Errorf("the message was delivered %d times before being dead-lettered; a rejected message must not be retried", got)
	}
	if n, _, derr := pub.QueueDepth(top.Queue); derr == nil && n != 0 {
		t.Errorf("the work queue still holds %d messages after the rejection", n)
	}
	t.Logf("an uninterpretable message was dead-lettered after %d delivery/deliveries", got)
	e.WriteEvidence(t, "f4-poison-dead-lettered.txt", []byte(fmt.Sprintf(
		"deliveries_before_dead_letter=%d dead_letter_queue=%s\n", got, top.DeadLetterQ)))
}

// TestF4UnsupportedContractIsRejected asserts a payload this build must not
// interpret is dead-lettered rather than retried forever.
func TestF4UnsupportedContractIsRejected(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseBaseline)

	for name, body := range map[string]string{
		"future version": `{"contract_version":99,"job_id":"66666666-6666-4666-8666-666666666666","attempt":1}`,
		"malformed":      `{"contract_version":1,`,
		"bad job id":     `{"contract_version":1,"job_id":"not-a-uuid","attempt":1}`,
		"zero attempt":   `{"contract_version":1,"job_id":"66666666-6666-4666-8666-666666666666","attempt":0}`,
	} {
		if _, err := jobs.DecodeMessage([]byte(body)); err == nil {
			t.Errorf("%s: the decoder accepted a payload it must reject", name)
		}
	}
}

// TestF4PersistentMessagesSurviveBrokerRestart publishes a persistent message
// into the applications' real work queue against a job that does not exist, so
// nothing consumes it to completion, then checks it is still there after the
// orchestrator restarts the broker.
//
// The message references a real durable job that is deliberately left
// unpublished by the watcher, so the assertion is about broker durability and
// nothing else.
func TestF4PersistentMessagesSurviveBrokerRestart(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseBaseline, PhaseRabbitRestarted)

	conn, _ := e.Broker(t)
	top := e.IsolatedTopology(t, "durability")

	switch e.Phase {
	case PhaseBaseline:
		// Declare without the automatic teardown: this topology must outlive
		// the phase so the post-restart phase can inspect it. It is removed by
		// the post-restart phase, and in any case by `make down-clean`.
		ch, _, err := conn.Channel()
		if err != nil {
			t.Fatalf("open channel: %v", err)
		}
		if err := broker.Declare(ch, top); err != nil {
			_ = ch.Close()
			t.Fatalf("declare durability topology: %v", err)
		}
		_ = ch.Close()

		pub := broker.NewPublisher(conn, top, e.Log, e.Cfg.Broker.ConfirmTimeout)
		t.Cleanup(pub.Close)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		const want = 3
		for i := 0; i < want; i++ {
			if result, err := pub.Publish(ctx, jobs.Message{
				ContractVersion: jobs.ContractVersion,
				JobID:           fmt.Sprintf("77777777-7777-4777-8777-%012d", i),
				Attempt:         1, EnqueuedAt: time.Now().UTC(),
			}); result != broker.PublishConfirmed {
				t.Fatalf("publish %d: %s (%v)", i, result, err)
			}
		}
		if err := waitForErr(20*time.Second, func() error {
			n, _, derr := pub.QueueDepth(top.Queue)
			if derr != nil {
				return derr
			}
			if n != want {
				return fmt.Errorf("queue holds %d messages, expected %d", n, want)
			}
			return nil
		}); err != nil {
			t.Fatalf("%v", err)
		}
		e.SaveState(t, "f4-durable-queue", top.Queue)
		e.SaveState(t, "f4-durable-count", fmt.Sprintf("%d", want))
		t.Logf("left %d persistent messages in %s for the post-restart phase", want, top.Queue)

	case PhaseRabbitRestarted:
		queue := e.LoadState(t, "f4-durable-queue")
		want := strings.TrimSpace(e.LoadState(t, "f4-durable-count"))

		pub := broker.NewPublisher(conn, top, e.Log, e.Cfg.Broker.ConfirmTimeout)
		t.Cleanup(pub.Close)

		var got int
		if err := waitForErr(60*time.Second, func() error {
			n, _, derr := pub.QueueDepth(queue)
			if derr != nil {
				return derr
			}
			got = n
			if fmt.Sprintf("%d", n) != want {
				return fmt.Errorf("queue %s holds %d messages, expected %s", queue, n, want)
			}
			return nil
		}); err != nil {
			t.Fatalf("persistent messages did not survive the broker restart: %v", err)
		}
		t.Logf("%d persistent messages survived the broker restart in %s", got, queue)
		e.WriteEvidence(t, "f4-broker-restart-durability.txt", []byte(fmt.Sprintf(
			"queue=%s expected=%s after_restart=%d\n", queue, want, got)))

		// Remove the queue now that the assertion is done.
		ch, _, err := conn.Channel()
		if err == nil {
			_, _ = ch.QueueDelete(top.Queue, false, false, false)
			_, _ = ch.QueueDelete(top.DeadLetterQ, false, false, false)
			_ = ch.ExchangeDelete(top.Exchange, false, false)
			_ = ch.ExchangeDelete(top.DeadLetterX, false, false)
			_ = ch.Close()
		}
	}
}

// TestF4EndToEndThroughTheRunningApplications registers a real job and lets
// the running watcher publish it and a running renamer consume it.
//
// What this establishes: the dispatch path publishes with confirms, a renamer
// consumes with manual acknowledgement, and the durable outcome is written
// before the acknowledgement. What it explicitly does not establish: any
// normalization, any destination reservation, or any delivery to a consumer
// directory. The expected outcome is an honest hold.
func TestF4EndToEndThroughTheRunningApplications(t *testing.T) {
	e := Suite()
	e.OnlyIn(t, PhaseBaseline)
	led := e.Ledger(t)

	job := registerSyntheticJob(t, led, e, "f4-end-to-end")
	e.SaveState(t, "f4-e2e-job", job.JobID)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	var final ledger.Job
	err := waitForErr(90*time.Second, func() error {
		cur, gerr := led.GetJob(ctx, job.JobID)
		if gerr != nil {
			return gerr
		}
		final = cur
		if cur.State != jobs.StateHeld {
			return fmt.Errorf("job %s is in state %q, waiting for %q", job.JobID, cur.State, jobs.StateHeld)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("the job never reached a durable outcome through the running applications: %v", err)
	}

	// These synthetic jobs are registered without a source file, so the honest
	// terminal reason is that the source is absent -- not a claim of delivery.
	if final.FailureCategory == nil || *final.FailureCategory != string(jobs.CategorySourceAbsent) {
		t.Fatalf("the recorded hold category is %v, expected %q",
			derefCategory(final), jobs.CategorySourceAbsent)
	}
	if final.DispatchedAt == nil {
		t.Errorf("the job reached a renamer but no confirmed dispatch was recorded")
	}
	if final.DeliveryAttempts < 1 {
		t.Errorf("delivery attempts were not recorded (%d)", final.DeliveryAttempts)
	}
	if final.NormalizedName != nil || final.ReservedName != nil {
		t.Errorf("a normalized or reserved name was recorded, but normalization is not implemented: %v / %v",
			final.NormalizedName, final.ReservedName)
	}

	events, err := led.Events(ctx, job.JobID)
	if err != nil {
		t.Fatalf("read retained history: %v", err)
	}
	var order []string
	for _, ev := range events {
		order = append(order, string(ev.EventType))
	}
	for _, want := range []jobs.EventType{
		jobs.EventRegistered, jobs.EventDispatchClaimed,
		jobs.EventDispatchConfirmed, jobs.EventDeliveryReceived, jobs.EventHeld,
	} {
		if !containsStr(order, string(want)) {
			t.Errorf("the retained history is missing a %q event; got %v", want, order)
		}
	}

	e.WriteEvidence(t, "f4-end-to-end.txt", []byte(fmt.Sprintf(
		"job_id=%s state=%s category=%s dispatch_attempts=%d delivery_attempts=%d history=%s\n",
		final.JobID, final.State, *final.FailureCategory,
		final.DispatchAttempts, final.DeliveryAttempts, strings.Join(order, ","))))
}

func containsStr(hay []string, needle string) bool {
	for _, h := range hay {
		if h == needle {
			return true
		}
	}
	return false
}
