package GoEventBus

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"cloud.google.com/go/pubsub"
)

type GCPPubSubProviderConfig struct {
	Client       *pubsub.Client
	Topic        string
	Subscription string

	// EnableOrdering enables Pub/Sub message ordering. OrderingKey defaults to
	// the event projection when this is true.
	EnableOrdering bool
	OrderingKey    func(Event) string

	Codec EventCodec
}

type GCPPubSubProvider struct {
	topic        *pubsub.Topic
	subscription *pubsub.Subscription
	codec        EventCodec
	orderingKey  func(Event) string
	closed       atomic.Bool
}

func WithGCPPubSub(config GCPPubSubProviderConfig) EventStoreOption {
	return withProviderFactory(func() (Provider, error) { return NewGCPPubSubProvider(config) })
}

func NewGCPPubSubProvider(config GCPPubSubProviderConfig) (*GCPPubSubProvider, error) {
	if config.Client == nil {
		return nil, errors.New("goeventbus: GCP Pub/Sub client must not be nil")
	}
	if config.Topic == "" {
		return nil, errors.New("goeventbus: GCP Pub/Sub topic must not be empty")
	}
	if config.Codec == nil {
		config.Codec = JSONCodec{}
	}

	topic := config.Client.Topic(config.Topic)
	topic.EnableMessageOrdering = config.EnableOrdering

	var subscription *pubsub.Subscription
	if config.Subscription != "" {
		subscription = config.Client.Subscription(config.Subscription)
	}

	return &GCPPubSubProvider{
		topic:        topic,
		subscription: subscription,
		codec:        config.Codec,
		orderingKey:  config.OrderingKey,
	}, nil
}

func (p *GCPPubSubProvider) Publish(ctx context.Context, e Event) error {
	if p.closed.Load() {
		return ErrProviderClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	projection, err := remoteProjection(e)
	if err != nil {
		return err
	}
	payload, err := p.codec.Encode(e)
	if err != nil {
		return err
	}

	msg := &pubsub.Message{
		Data: payload,
		Attributes: map[string]string{
			"goeventbus-projection": projection,
			"goeventbus-event-id":   e.ID,
		},
	}
	if p.topic.EnableMessageOrdering {
		if p.orderingKey != nil {
			msg.OrderingKey = p.orderingKey(e)
		} else {
			msg.OrderingKey = projection
		}
	}

	if _, err := p.topic.Publish(ctx, msg).Get(ctx); err != nil {
		return fmt.Errorf("goeventbus: publish GCP Pub/Sub event: %w", err)
	}
	return nil
}

func (p *GCPPubSubProvider) Consume(ctx context.Context, consumer EventConsumer) error {
	if consumer == nil {
		return ErrNilConsumer
	}
	if p.closed.Load() {
		return ErrProviderClosed
	}
	if p.subscription == nil {
		return errors.New("goeventbus: GCP Pub/Sub subscription is required to consume events")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	err := p.subscription.Receive(ctx, func(messageCtx context.Context, msg *pubsub.Message) {
		if p.closed.Load() {
			msg.Nack()
			return
		}
		event, err := p.codec.Decode(msg.Data)
		if err != nil {
			msg.Nack()
			return
		}
		if err := consumer(messageCtx, event); err != nil {
			msg.Nack()
			return
		}
		msg.Ack()
	})
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if p.closed.Load() {
			return ErrProviderClosed
		}
		return fmt.Errorf("goeventbus: consume GCP Pub/Sub events: %w", err)
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if p.closed.Load() {
		return ErrProviderClosed
	}
	return nil
}

func (p *GCPPubSubProvider) Close() error {
	if p.closed.Swap(true) {
		return nil
	}
	p.topic.Stop()
	return nil
}

var _ Provider = (*GCPPubSubProvider)(nil)
