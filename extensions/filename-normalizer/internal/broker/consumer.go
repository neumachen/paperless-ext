package broker

import (
	"context"
	"log/slog"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/logging"
)

// Decision is what a handler asks the consumer to do with a delivery.
type Decision int

const (
	// Ack settles the delivery. It must only be returned after a safe outcome
	// has been durably recorded.
	Ack Decision = iota
	// NackRequeue returns the delivery for another attempt. The broker's
	// x-delivery-limit bounds how many times this can happen before the
	// message is dead-lettered.
	NackRequeue
	// Reject discards the delivery to the dead-letter exchange without a
	// further attempt. It is for messages this build must never interpret.
	Reject
)

// Delivery is the handler's view of a message.
type Delivery struct {
	Body          []byte
	DeliveryTag   uint64
	Redelivered   bool
	MessageID     string
	DeliveryCount int
}

// Handler processes one delivery and decides its fate.
type Handler func(ctx context.Context, d Delivery) Decision

// ConsumerOptions configures a consumer.
type ConsumerOptions struct {
	Queue string
	// Prefetch bounds unacknowledged deliveries held by this instance.
	Prefetch int
	// Concurrency bounds simultaneously running handlers.
	Concurrency int
	ConsumerTag string
	Logger      *slog.Logger
	// OnActive reports whether a consumer is currently registered.
	OnActive func(active bool)
	// OnDelivery is called once per received delivery before the handler runs.
	OnDelivery func(d Delivery)
	// OnSettled is called with the decision and the handler duration.
	OnSettled func(d Delivery, decision Decision, took time.Duration)
	// Gate is consulted before each attach. While it reports false the
	// consumer stays detached, which is how the application avoids taking
	// deliveries it cannot durably settle.
	Gate func(ctx context.Context) (ok bool, category string)
	// DetachBackoff is how long the consumer stays detached after a gate
	// refusal or an explicit detach request.
	DetachBackoff time.Duration
	// HandlerBudget bounds a single handler's own work.
	//
	// A handler runs on a context derived with context.WithoutCancel from the
	// consume context, so cancelling the consumer at shutdown stops new
	// deliveries without aborting a delivery already being settled. Without
	// that, a SIGTERM would cancel the durable write of work already taken,
	// turning a graceful drain into a forced redelivery.
	HandlerBudget time.Duration
}

// Consumer runs a manual-acknowledgement consumer with bounded concurrency.
type Consumer struct {
	conn *Connection
	opts ConsumerOptions

	mu       sync.Mutex
	inflight int

	// detach carries an explicit request to stop consuming for a while. A
	// handler that cannot durably settle its delivery uses it so the instance
	// stops taking work instead of returning the same message in a tight loop.
	detach chan struct{}
}

// NewConsumer builds a consumer bound to a connection supervisor.
func NewConsumer(conn *Connection, opts ConsumerOptions) *Consumer {
	if opts.Concurrency < 1 {
		opts.Concurrency = 1
	}
	if opts.Prefetch < opts.Concurrency {
		opts.Prefetch = opts.Concurrency
	}
	if opts.DetachBackoff <= 0 {
		opts.DetachBackoff = 2 * time.Second
	}
	if opts.HandlerBudget <= 0 {
		opts.HandlerBudget = 30 * time.Second
	}
	return &Consumer{conn: conn, opts: opts, detach: make(chan struct{}, 1)}
}

// RequestDetach asks the consumer to stop consuming and wait out its backoff
// before reattaching.
//
// This is the bounded response to a dependency the handler needs but cannot
// reach. Returning the delivery and immediately accepting it again would spin:
// on RabbitMQ 4.x an explicit requeue does not advance the queue's delivery
// counter, so the broker's own delivery limit cannot bound that loop.
func (c *Consumer) RequestDetach() {
	select {
	case c.detach <- struct{}{}:
	default:
	}
}

// Run consumes until ctx is cancelled, reattaching after a connection loss.
//
// On shutdown it cancels the consumer first and then waits for in-flight
// handlers, so no delivery is abandoned unacknowledged by choice. Deliveries
// still unacknowledged when the connection actually drops are redelivered by
// the broker, which is the intended at-least-once behaviour.
func (c *Consumer) Run(ctx context.Context, h Handler) {
	gateRefused := false
	for ctx.Err() == nil {
		if c.opts.Gate != nil {
			gctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			ok, category := c.opts.Gate(gctx)
			cancel()
			if !ok {
				if !gateRefused {
					// Logged once per refusal streak so a long outage does not
					// bury the rest of the output.
					c.opts.Logger.Warn("not consuming: a dependency needed to settle deliveries is unavailable",
						slog.String("event", "consumer_gated"),
						slog.String("queue", c.opts.Queue),
						slog.String("category", category))
					gateRefused = true
				}
				c.setActive(false)
				if !sleepCtx(ctx, c.opts.DetachBackoff) {
					return
				}
				continue
			}
			if gateRefused {
				c.opts.Logger.Info("dependency is available again; resuming consumption",
					slog.String("event", "consumer_ungated"),
					slog.String("queue", c.opts.Queue))
				gateRefused = false
			}
		}

		err := c.consumeOnce(ctx, h)
		c.setActive(false)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			c.opts.Logger.Warn("consumer detached",
				slog.String("event", "consumer_detached"),
				slog.String("queue", c.opts.Queue),
				slog.String("error_kind", logging.ErrorKind(err)))
		}
		if !sleepCtx(ctx, c.opts.DetachBackoff) {
			return
		}
	}
	c.setActive(false)
}

func (c *Consumer) consumeOnce(ctx context.Context, h Handler) error {
	ch, _, err := c.conn.Channel()
	if err != nil {
		return err
	}
	defer func() { _ = ch.Close() }()

	if err := ch.Qos(c.opts.Prefetch, 0, false); err != nil {
		return err
	}

	deliveries, err := ch.Consume(
		c.opts.Queue, c.opts.ConsumerTag,
		false, // manual acknowledgement
		false, // not exclusive: several instances share the queue
		false, false, nil)
	if err != nil {
		return err
	}

	c.setActive(true)
	c.opts.Logger.Info("consumer attached",
		slog.String("event", "consumer_attached"),
		slog.String("queue", c.opts.Queue),
		slog.String("consumer_tag", c.opts.ConsumerTag),
		slog.Int("prefetch", c.opts.Prefetch),
		slog.Int("concurrency", c.opts.Concurrency))

	closed := ch.NotifyClose(make(chan *amqp.Error, 1))
	sem := make(chan struct{}, c.opts.Concurrency)
	var wg sync.WaitGroup

	defer func() {
		// Stop new deliveries, then let running handlers finish. Cancel can
		// fail if the channel is already gone, which is not an error here.
		_ = ch.Cancel(c.opts.ConsumerTag, false)
		if n := c.InFlight(); n > 0 {
			c.opts.Logger.Info("waiting for in-flight deliveries to settle",
				slog.String("event", "consumer_draining"),
				slog.String("queue", c.opts.Queue),
				slog.Int("count", n))
		}
		wg.Wait()
		c.setActive(false)
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-c.detach:
			// Stop taking deliveries. Anything in flight finishes in the
			// deferred wait below before the attachment is torn down.
			c.opts.Logger.Warn("detaching from the queue on request",
				slog.String("event", "consumer_detach_requested"),
				slog.String("queue", c.opts.Queue),
				slog.Int64("interval_ms", c.opts.DetachBackoff.Milliseconds()))
			return nil
		case amqpErr := <-closed:
			if amqpErr != nil {
				return amqpErr
			}
			return ErrNotConnected
		case msg, ok := <-deliveries:
			if !ok {
				return ErrNotConnected
			}
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				// Capacity was not available and shutdown began. Return the
				// delivery so another instance can take it immediately.
				_ = msg.Nack(false, true)
				return nil
			}
			wg.Add(1)
			go func(msg amqp.Delivery) {
				defer wg.Done()
				defer func() { <-sem }()
				c.handle(ctx, h, msg)
			}(msg)
		}
	}
}

func (c *Consumer) handle(ctx context.Context, h Handler, msg amqp.Delivery) {
	d := Delivery{
		Body:          msg.Body,
		DeliveryTag:   msg.DeliveryTag,
		Redelivered:   msg.Redelivered,
		MessageID:     msg.MessageId,
		DeliveryCount: deliveryCount(msg.Headers),
	}
	if c.opts.OnDelivery != nil {
		c.opts.OnDelivery(d)
	}
	c.track(1)
	start := time.Now()

	// Detached from the consume context on purpose: a delivery already taken
	// must be allowed to reach a durable outcome even though shutdown has
	// begun. The budget keeps that bounded.
	hctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.opts.HandlerBudget)
	decision := h(hctx, d)
	cancel()

	took := time.Since(start)
	c.track(-1)

	var err error
	switch decision {
	case Ack:
		err = msg.Ack(false)
	case NackRequeue:
		err = msg.Nack(false, true)
	case Reject:
		err = msg.Reject(false)
	}
	if err != nil {
		// A failed settlement means the broker never learned the decision. The
		// message will be redelivered; the durable outcome already recorded by
		// the handler is what makes that safe.
		c.opts.Logger.Warn("delivery settlement failed",
			slog.String("event", "delivery_settlement_failed"),
			slog.String("queue", c.opts.Queue),
			slog.String("error_kind", logging.ErrorKind(err)))
	}
	if c.opts.OnSettled != nil {
		c.opts.OnSettled(d, decision, took)
	}
}

func (c *Consumer) track(delta int) {
	c.mu.Lock()
	c.inflight += delta
	c.mu.Unlock()
}

// InFlight reports handlers currently running.
func (c *Consumer) InFlight() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.inflight
}

func (c *Consumer) setActive(v bool) {
	if c.opts.OnActive != nil {
		c.opts.OnActive(v)
	}
}

// deliveryCount reads the quorum-queue redelivery counter when present.
func deliveryCount(h amqp.Table) int {
	if h == nil {
		return 0
	}
	switch v := h["x-delivery-count"].(type) {
	case int32:
		return int(v)
	case int64:
		return int(v)
	case int:
		return v
	default:
		return 0
	}
}
