package GoEventBus

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// RabbitMQProviderConfig configures a RabbitMQ topic-exchange provider.
// Queue is optional for publish-only instances. It is required by Consume.
type RabbitMQProviderConfig struct {
	URL        string
	Exchange   string
	Queue      string
	BindingKey string
	Consumer   string

	// PrefetchCount limits unacknowledged deliveries for each Consume call. It
	// defaults to one to apply back-pressure at the broker.
	PrefetchCount int
	Codec         EventCodec
}

// RabbitMQProvider publishes Events to a durable topic exchange. It declares
// its configured queue and binding during construction when Queue is set.
// Publish waits for RabbitMQ publisher confirms before returning success.
type RabbitMQProvider struct {
	connection *amqp.Connection
	publisher  *amqp.Channel

	exchange   string
	queue      string
	bindingKey string
	consumer   string
	prefetch   int
	codec      EventCodec

	publishMu sync.Mutex
	closed    atomic.Bool
}

// WithRabbitMQ configures RabbitMQ as the EventStore provider. The provider
// is created lazily on first PublishToProvider or Consume call.
func WithRabbitMQ(config RabbitMQProviderConfig) EventStoreOption {
	return withProviderFactory(func() (Provider, error) {
		return NewRabbitMQProvider(config)
	})
}

// NewRabbitMQProvider connects to RabbitMQ, enables publisher confirms, and
// declares the configured topic-exchange topology. The provider owns and
// closes the AMQP connection.
func NewRabbitMQProvider(config RabbitMQProviderConfig) (*RabbitMQProvider, error) {
	if config.URL == "" {
		return nil, errors.New("goeventbus: RabbitMQ URL must not be empty")
	}
	if config.Exchange == "" {
		return nil, errors.New("goeventbus: RabbitMQ exchange must not be empty")
	}
	if config.BindingKey == "" {
		config.BindingKey = "#"
	}
	if config.PrefetchCount <= 0 {
		config.PrefetchCount = 1
	}
	if config.Codec == nil {
		config.Codec = JSONCodec{}
	}

	connection, err := amqp.Dial(config.URL)
	if err != nil {
		return nil, fmt.Errorf("goeventbus: connect to RabbitMQ: %w", err)
	}
	publisher, err := connection.Channel()
	if err != nil {
		_ = connection.Close()
		return nil, fmt.Errorf("goeventbus: open RabbitMQ publisher channel: %w", err)
	}
	provider := &RabbitMQProvider{
		connection: connection,
		publisher:  publisher,
		exchange:   config.Exchange,
		queue:      config.Queue,
		bindingKey: config.BindingKey,
		consumer:   config.Consumer,
		prefetch:   config.PrefetchCount,
		codec:      config.Codec,
	}
	if err := provider.declareTopology(publisher); err != nil {
		_ = publisher.Close()
		_ = connection.Close()
		return nil, err
	}
	if err := publisher.Confirm(false); err != nil {
		_ = publisher.Close()
		_ = connection.Close()
		return nil, fmt.Errorf("goeventbus: enable RabbitMQ publisher confirms: %w", err)
	}
	return provider, nil
}

// Publish publishes e with its Projection as the topic routing key. It waits
// for the broker confirmation, so a nil error means RabbitMQ accepted the
// message (not that a consumer has finished processing it).
func (p *RabbitMQProvider) Publish(ctx context.Context, e Event) error {
	if p.closed.Load() {
		return ErrProviderClosed
	}
	routingKey, err := remoteProjection(e)
	if err != nil {
		return err
	}
	payload, err := p.codec.Encode(e)
	if err != nil {
		return err
	}

	p.publishMu.Lock()
	defer p.publishMu.Unlock()
	if p.closed.Load() {
		return ErrProviderClosed
	}
	confirmation, err := p.publisher.PublishWithDeferredConfirmWithContext(ctx,
		p.exchange,
		routingKey,
		false,
		false,
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			MessageId:    e.ID,
			Type:         routingKey,
			Timestamp:    time.Now().UTC(),
			Body:         payload,
		},
	)
	if err != nil {
		return fmt.Errorf("goeventbus: publish RabbitMQ event: %w", err)
	}
	if confirmation == nil {
		return errors.New("goeventbus: RabbitMQ publisher confirmation is unavailable")
	}
	confirmed, err := confirmation.WaitContext(ctx)
	if err != nil {
		return fmt.Errorf("goeventbus: wait for RabbitMQ publisher confirmation: %w", err)
	}
	if !confirmed {
		return errors.New("goeventbus: RabbitMQ did not confirm event publication")
	}
	return nil
}

// Consume receives deliveries from the configured queue. A delivery is acked
// only after consumer returns nil; decode and consumer errors are requeued.
func (p *RabbitMQProvider) Consume(ctx context.Context, consumer EventConsumer) error {
	if consumer == nil {
		return ErrNilConsumer
	}
	if p.closed.Load() {
		return ErrProviderClosed
	}
	if p.queue == "" {
		return errors.New("goeventbus: RabbitMQ queue is required to consume events")
	}

	channel, err := p.connection.Channel()
	if err != nil {
		return fmt.Errorf("goeventbus: open RabbitMQ consumer channel: %w", err)
	}
	defer channel.Close()
	if err := channel.Qos(p.prefetch, 0, false); err != nil {
		return fmt.Errorf("goeventbus: configure RabbitMQ prefetch: %w", err)
	}
	deliveries, err := channel.ConsumeWithContext(ctx, p.queue, p.consumer, false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("goeventbus: consume RabbitMQ queue: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case delivery, ok := <-deliveries:
			if !ok {
				if p.closed.Load() {
					return ErrProviderClosed
				}
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return errors.New("goeventbus: RabbitMQ delivery channel closed")
			}
			event, err := p.codec.Decode(delivery.Body)
			if err != nil {
				_ = delivery.Nack(false, true)
				return fmt.Errorf("goeventbus: decode RabbitMQ event: %w", err)
			}
			if err := consumer(ctx, event); err != nil {
				_ = delivery.Nack(false, true)
				return err
			}
			if err := delivery.Ack(false); err != nil {
				return fmt.Errorf("goeventbus: acknowledge RabbitMQ event: %w", err)
			}
		}
	}
}

// Close closes the provider's channel and AMQP connection. It is safe to call
// more than once.
func (p *RabbitMQProvider) Close() error {
	if p.closed.Swap(true) {
		return nil
	}
	p.publishMu.Lock()
	defer p.publishMu.Unlock()
	channelErr := p.publisher.Close()
	connectionErr := p.connection.Close()
	return errors.Join(channelErr, connectionErr)
}

func (p *RabbitMQProvider) declareTopology(channel *amqp.Channel) error {
	if err := channel.ExchangeDeclare(p.exchange, "topic", true, false, false, false, nil); err != nil {
		return fmt.Errorf("goeventbus: declare RabbitMQ exchange: %w", err)
	}
	if p.queue == "" {
		return nil
	}
	if _, err := channel.QueueDeclare(p.queue, true, false, false, false, nil); err != nil {
		return fmt.Errorf("goeventbus: declare RabbitMQ queue: %w", err)
	}
	if err := channel.QueueBind(p.queue, p.bindingKey, p.exchange, false, nil); err != nil {
		return fmt.Errorf("goeventbus: bind RabbitMQ queue: %w", err)
	}
	return nil
}

var _ Provider = (*RabbitMQProvider)(nil)
