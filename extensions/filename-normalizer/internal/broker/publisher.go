package broker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/jobs"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/logging"
)

// PublishResult reports what the broker actually said about a publication.
type PublishResult string

const (
	// PublishConfirmed means the broker acknowledged the publication.
	PublishConfirmed PublishResult = "confirmed"
	// PublishUnconfirmed means no confirm arrived in time, or the broker
	// nacked. The publication outcome is unknown and must not be recorded as
	// dispatched.
	PublishUnconfirmed PublishResult = "unconfirmed"
	// PublishReturned means the message was unroutable and came back. The
	// topology is wrong; the job stays pending rather than being lost.
	PublishReturned PublishResult = "returned"
	// PublishError means the publication could not be attempted.
	PublishError PublishResult = "publish_error"
)

// ErrUnconfirmed reports a publication whose outcome the broker did not settle.
var ErrUnconfirmed = errors.New("publication was not confirmed")

// ErrReturned reports an unroutable publication.
var ErrReturned = errors.New("publication was returned as unroutable")

// Publisher publishes job references with publisher confirms enabled.
//
// The channel is opened lazily and reopened whenever the underlying connection
// is replaced, so a reconnect does not leave a publisher writing into a dead
// channel.
type Publisher struct {
	conn     *Connection
	topology Topology
	log      *slog.Logger
	timeout  time.Duration

	mu      sync.Mutex
	ch      *amqp.Channel
	gen     uint64
	returns chan amqp.Return
}

// NewPublisher builds a publisher bound to a connection supervisor.
func NewPublisher(conn *Connection, t Topology, log *slog.Logger, confirmTimeout time.Duration) *Publisher {
	return &Publisher{conn: conn, topology: t, log: log, timeout: confirmTimeout}
}

func (p *Publisher) channel() (*amqp.Channel, chan amqp.Return, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	gen := p.conn.Generation()
	if p.ch != nil && !p.ch.IsClosed() && p.gen == gen {
		return p.ch, p.returns, nil
	}
	if p.ch != nil && !p.ch.IsClosed() {
		_ = p.ch.Close()
	}
	p.ch = nil

	ch, gen, err := p.conn.Channel()
	if err != nil {
		return nil, nil, err
	}
	// Confirm mode is required: without it a successful Publish call says
	// nothing about whether the broker durably accepted the message.
	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()
		return nil, nil, fmt.Errorf("enable publisher confirms: %w", err)
	}
	p.ch = ch
	p.gen = gen
	p.returns = ch.NotifyReturn(make(chan amqp.Return, 16))
	return p.ch, p.returns, nil
}

// Publish sends one job reference and waits for the broker to settle it.
//
// The message is persistent and published mandatory, so an unroutable message
// is reported rather than silently discarded. The caller must treat anything
// other than PublishConfirmed as "not dispatched".
func (p *Publisher) Publish(ctx context.Context, msg jobs.Message) (PublishResult, error) {
	body, err := msg.Encode()
	if err != nil {
		return PublishError, err
	}

	ch, returns, err := p.channel()
	if err != nil {
		return PublishError, err
	}

	// Drain any return left over from an earlier publication so a stale entry
	// cannot be attributed to this one.
	//
	// The drain must stop when the channel is closed. A closed Go channel is
	// permanently ready to receive, so a loop that ignores the second receive
	// value spins forever the moment the AMQP client tears the notification
	// channel down, which it does whenever the connection or channel closes.
	// That would hang this call past its own timeout and, because the dispatch
	// worker also performs stranded-claim recovery, would stall dispatch even
	// after the broker came back.
	if !drainReturns(returns) {
		p.invalidate()
		return PublishError, fmt.Errorf("%w: return channel closed before publishing", ErrNotConnected)
	}

	pubCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	conf, err := ch.PublishWithDeferredConfirmWithContext(pubCtx,
		p.topology.Exchange, p.topology.RoutingKey,
		true,  // mandatory: an unroutable message must come back
		false, // immediate is not supported by modern brokers
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			MessageId:    msg.JobID,
			Timestamp:    msg.EnqueuedAt,
			Type:         "filename_normalizer.job",
			AppId:        "filename-normalizer",
			Headers: amqp.Table{
				"x-contract-version": int32(msg.ContractVersion),
			},
			Body: body,
		})
	if err != nil {
		p.invalidate()
		return PublishError, err
	}

	acked, err := conf.WaitContext(pubCtx)
	if err != nil {
		p.invalidate()
		return PublishUnconfirmed, fmt.Errorf("%w: %v", ErrUnconfirmed, logging.ErrorKind(err))
	}
	if !acked {
		return PublishUnconfirmed, ErrUnconfirmed
	}

	// A returned message is still confirmed by the broker, so the return
	// channel must be checked after the confirm to detect an unroutable send.
	//
	// The check is non-blocking, and that is sound rather than a shortcut. For
	// an unroutable mandatory publication the broker sends basic.return before
	// basic.ack, and the AMQP client processes incoming frames in order on one
	// dispatch goroutine: the return is placed in this buffered channel before
	// the frame that settles the confirmation is handled. So once WaitContext
	// has returned, a return for this publication is already buffered.
	//
	// It previously waited 50ms unconditionally instead, which capped a
	// publisher at roughly twenty publications a second and was the reason the
	// dispatch worker moved so little work per pass.
	select {
	case ret, open := <-returns:
		if !open {
			// The channel went away before routability could be established.
			// The broker did acknowledge the publication, but an unroutable
			// return would now be unobservable, so this is reported as
			// unconfirmed: the job stays pending and is republished under the
			// same identity. Duplicate messages are allowed; a silently
			// discarded one would not be.
			p.invalidate()
			return PublishUnconfirmed, fmt.Errorf(
				"%w: return channel closed before routability could be established", ErrUnconfirmed)
		}
		if ret.MessageId == msg.JobID {
			p.log.Error("publication returned as unroutable",
				slog.String("event", "publish_returned"),
				slog.String("job_id", msg.JobID),
				slog.String("exchange", p.topology.Exchange),
				slog.String("routing_key", p.topology.RoutingKey))
			return PublishReturned, ErrReturned
		}
	default:
	}

	return PublishConfirmed, nil
}

// drainReturns discards buffered returns and reports whether the channel is
// still open. It returns false as soon as the channel is closed, which is the
// only way to distinguish "nothing buffered" from "channel torn down".
func drainReturns(returns chan amqp.Return) (open bool) {
	for {
		select {
		case _, ok := <-returns:
			if !ok {
				return false
			}
		default:
			return true
		}
	}
}

func (p *Publisher) invalidate() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ch != nil && !p.ch.IsClosed() {
		_ = p.ch.Close()
	}
	p.ch = nil
}

// Close releases the publisher channel.
func (p *Publisher) Close() { p.invalidate() }

// QueueDepth reports the broker's current ready-message count for the work
// queue. It is real broker state used as integration evidence.
func (p *Publisher) QueueDepth(queue string) (messages, consumers int, err error) {
	ch, _, err := p.conn.Channel()
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = ch.Close() }()
	q, err := ch.QueueDeclarePassive(queue, true, false, false, false, nil)
	if err != nil {
		return 0, 0, err
	}
	return q.Messages, q.Consumers, nil
}
