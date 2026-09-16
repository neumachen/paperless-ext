package broker

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/config"
	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/logging"
)

// ErrNotConnected reports that no live channel is currently held.
var ErrNotConnected = errors.New("broker connection is not established")

// Options configures a Connection.
type Options struct {
	Config config.Broker
	Logger *slog.Logger
	// OnReconnect is invoked after every successful (re)establishment except
	// the first, so metrics can count real reconnections.
	OnReconnect func()
	// OnUp is invoked whenever connectivity changes.
	OnUp func(up bool)
	// Name is the connection name shown in the broker's management view. It
	// identifies the instance and never contains a credential.
	Name string
}

// Connection maintains a single AMQP connection and redeclares the topology on
// every establishment. It owns reconnection so callers see either a usable
// channel or ErrNotConnected, never a half-dead one.
type Connection struct {
	opts     Options
	topology Topology

	mu        sync.RWMutex
	conn      *amqp.Connection
	connected bool
	gen       uint64

	established int
	closeOnce   sync.Once
	done        chan struct{}
}

// Dial builds the supervisor. Run must be called to establish connectivity.
func Dial(opts Options) *Connection {
	return &Connection{
		opts:     opts,
		topology: TopologyFromConfig(opts.Config),
		done:     make(chan struct{}),
	}
}

// Run keeps the connection alive until ctx is cancelled. It returns only after
// the connection is closed, so callers run it in its own goroutine.
func (c *Connection) Run(ctx context.Context) {
	defer close(c.done)
	for {
		if ctx.Err() != nil {
			return
		}
		closed, err := c.connectOnce(ctx)
		if err != nil {
			c.setConnected(false)
			c.opts.Logger.Warn("broker connection attempt failed",
				slog.String("event", "broker_connect_failed"),
				slog.String("dependency", "rabbitmq"),
				slog.String("host", c.opts.Config.Host),
				slog.Int("port", c.opts.Config.Port),
				slog.String("error_kind", logging.ErrorKind(err)))
			if !sleepCtx(ctx, c.opts.Config.ReconnectDelay) {
				return
			}
			continue
		}

		select {
		case <-ctx.Done():
			c.closeConn()
			return
		case reason := <-closed:
			c.setConnected(false)
			kind := "connection_lost"
			if reason != nil {
				kind = logging.ErrorKind(reason)
			}
			c.opts.Logger.Warn("broker connection lost",
				slog.String("event", "broker_connection_lost"),
				slog.String("dependency", "rabbitmq"),
				slog.String("error_kind", kind))
			if !sleepCtx(ctx, c.opts.Config.ReconnectDelay) {
				return
			}
		}
	}
}

func (c *Connection) connectOnce(ctx context.Context) (chan *amqp.Error, error) {
	conn, err := amqp.DialConfig(c.opts.Config.URI(), amqp.Config{
		Heartbeat:  c.opts.Config.Heartbeat,
		Dial:       amqp.DefaultDial(c.opts.Config.DialTimeout),
		Properties: amqp.Table{"connection_name": c.opts.Name},
	})
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	if err := Declare(ch, c.topology); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, err
	}
	_ = ch.Close()

	c.mu.Lock()
	c.conn = conn
	c.connected = true
	c.gen++
	c.established++
	first := c.established == 1
	c.mu.Unlock()

	if c.opts.OnUp != nil {
		c.opts.OnUp(true)
	}
	if !first && c.opts.OnReconnect != nil {
		c.opts.OnReconnect()
	}
	c.opts.Logger.Info("broker connected",
		slog.String("event", "broker_connected"),
		slog.String("dependency", "rabbitmq"),
		slog.String("host", c.opts.Config.Host),
		slog.Int("port", c.opts.Config.Port),
		slog.String("vhost", c.opts.Config.VHost),
		slog.String("exchange", c.topology.Exchange),
		slog.String("queue", c.topology.Queue),
		slog.String("dlx", c.topology.DeadLetterX),
		slog.Int("max_attempts", c.topology.DeliveryLimit))

	return conn.NotifyClose(make(chan *amqp.Error, 1)), nil
}

func (c *Connection) setConnected(v bool) {
	c.mu.Lock()
	changed := c.connected != v
	c.connected = v
	c.mu.Unlock()
	if changed && c.opts.OnUp != nil {
		c.opts.OnUp(v)
	}
}

func (c *Connection) closeConn() {
	c.mu.Lock()
	conn := c.conn
	c.conn = nil
	c.connected = false
	c.mu.Unlock()
	if conn != nil && !conn.IsClosed() {
		_ = conn.Close()
	}
}

// Connected reports live connectivity as of the last event.
func (c *Connection) Connected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.connected && c.conn != nil && !c.conn.IsClosed()
}

// Channel opens a fresh channel on the live connection.
func (c *Connection) Channel() (*amqp.Channel, uint64, error) {
	c.mu.RLock()
	conn, gen, ok := c.conn, c.gen, c.connected
	c.mu.RUnlock()
	if !ok || conn == nil || conn.IsClosed() {
		return nil, 0, ErrNotConnected
	}
	ch, err := conn.Channel()
	if err != nil {
		return nil, 0, err
	}
	return ch, gen, nil
}

// Generation reports a counter that increments on every establishment. A
// caller can detect that its channel belongs to a superseded connection.
func (c *Connection) Generation() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.gen
}

// ForceClose drops the current connection without stopping the supervisor.
//
// This is a real interruption of the application's own connection, used by the
// integration suite to observe redelivery of an unacknowledged message. It
// substitutes nothing: the broker and the client library are the real ones.
func (c *Connection) ForceClose() {
	c.mu.RLock()
	conn := c.conn
	c.mu.RUnlock()
	if conn != nil && !conn.IsClosed() {
		_ = conn.Close()
	}
}

// Wait blocks until Run has returned.
func (c *Connection) Wait(ctx context.Context) {
	select {
	case <-c.done:
	case <-ctx.Done():
	}
}

// Shutdown closes the connection.
func (c *Connection) Shutdown() {
	c.closeOnce.Do(c.closeConn)
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
