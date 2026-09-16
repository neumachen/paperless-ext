// Package broker is the RabbitMQ integration used by both applications.
//
// The topology is durable and declared identically by every process, so the
// first one to start creates it and the rest attach to it. Messages carry an
// opaque job reference only.
package broker

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/neumachen/paperless-ext/extensions/filename-normalizer/internal/config"
)

// Topology is the declared exchange/queue arrangement.
type Topology struct {
	Exchange      string
	Queue         string
	RoutingKey    string
	DeadLetterX   string
	DeadLetterQ   string
	DeliveryLimit int
}

// TopologyFromConfig lifts the configured names.
func TopologyFromConfig(c config.Broker) Topology {
	return Topology{
		Exchange:      c.Exchange,
		Queue:         c.Queue,
		RoutingKey:    c.RoutingKey,
		DeadLetterX:   c.DeadLetterX,
		DeadLetterQ:   c.DeadLetterQ,
		DeliveryLimit: c.DeliveryLimit,
	}
}

// Declare creates the durable topology.
//
// The work queue is a quorum queue: it is durable by construction and it
// supports x-delivery-limit, which is how bounded retries are enforced at the
// broker rather than by a counter the application could lose. Exhausted
// messages are dead-lettered, and a dead-lettered message is a visible
// artefact, not a substitute for the job ledger.
func Declare(ch *amqp.Channel, t Topology) error {
	if err := ch.ExchangeDeclare(t.DeadLetterX, "topic", true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare dead-letter exchange: %w", err)
	}
	if _, err := ch.QueueDeclare(t.DeadLetterQ, true, false, false, false, amqp.Table{
		"x-queue-type": "quorum",
	}); err != nil {
		return fmt.Errorf("declare dead-letter queue: %w", err)
	}
	if err := ch.QueueBind(t.DeadLetterQ, "#", t.DeadLetterX, false, nil); err != nil {
		return fmt.Errorf("bind dead-letter queue: %w", err)
	}

	if err := ch.ExchangeDeclare(t.Exchange, "direct", true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare exchange: %w", err)
	}
	if _, err := ch.QueueDeclare(t.Queue, true, false, false, false, amqp.Table{
		"x-queue-type":              "quorum",
		"x-dead-letter-exchange":    t.DeadLetterX,
		"x-dead-letter-routing-key": t.RoutingKey,
		"x-delivery-limit":          int32(t.DeliveryLimit),
	}); err != nil {
		return fmt.Errorf("declare work queue: %w", err)
	}
	if err := ch.QueueBind(t.Queue, t.RoutingKey, t.Exchange, false, nil); err != nil {
		return fmt.Errorf("bind work queue: %w", err)
	}
	return nil
}
