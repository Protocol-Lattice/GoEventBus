package GoEventBus

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/apache/pulsar-client-go/pulsar"
)

type PulsarProviderConfig struct {
	Client       pulsar.Client
	Topic        string
	Subscription string

	// SubscriptionType defaults to Shared. Set explicitly for Exclusive,
	// Failover, or KeyShared behavior.
	SubscriptionType *pulsar.SubscriptionType
	Codec            EventCodec
}

type PulsarProvider struct {
	client pulsar.Client
	cfg    PulsarProviderConfig
	codec  EventCodec

	mu       sync.Mutex
	producer pulsar.Producer
	consumer pulsar.Consumer
	closed   atomic.Bool
}

func WithPulsar(config PulsarProviderConfig) EventStoreOption {
	return withProviderFactory(func() (Provider, error) { return NewPulsarProvider(config) })
}

func NewPulsarProvider(config PulsarProviderConfig) (*PulsarProvider, error) {
	if config.Client == nil {
		return nil, errors.New("goeventbus: Pulsar client must not be nil")
	}
	if config.Topic == "" {
		return nil, errors.New("goeventbus: Pulsar topic must not be empty")
	}
	if config.Codec == nil {
		config.Codec = JSONCodec{}
	}
	return &PulsarProvider{client: config.Client, cfg: config, codec: config.Codec}, nil
}

func (p *PulsarProvider) Publish(ctx context.Context, e Event) error {
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
	producer, err := p.producerForUse()
	if err != nil {
		return err
	}
	_, err = producer.Send(ctx, &pulsar.ProducerMessage{
		Payload: payload,
		Key:     projection,
		Properties: map[string]string{
			"goeventbus-event-id":   e.ID,
			"goeventbus-projection": projection,
		},
	})
	if err != nil {
		return fmt.Errorf("goeventbus: publish Pulsar event: %w", err)
	}
	return nil
}

func (p *PulsarProvider) Consume(ctx context.Context, consumer EventConsumer) error {
	if consumer == nil {
		return ErrNilConsumer
	}
	if p.closed.Load() {
		return ErrProviderClosed
	}
	if p.cfg.Subscription == "" {
		return errors.New("goeventbus: Pulsar subscription is required to consume events")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	pc, err := p.consumerForUse()
	if err != nil {
		return err
	}
	for {
		if p.closed.Load() {
			return ErrProviderClosed
		}
		msg, err := pc.Receive(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if p.closed.Load() {
				return ErrProviderClosed
			}
			return fmt.Errorf("goeventbus: receive Pulsar event: %w", err)
		}
		event, err := p.codec.Decode(msg.Payload())
		if err != nil {
			pc.Nack(msg)
			return fmt.Errorf("goeventbus: decode Pulsar event: %w", err)
		}
		if err := consumer(ctx, event); err != nil {
			pc.Nack(msg)
			return err
		}
		if err := pc.Ack(msg); err != nil {
			return fmt.Errorf("goeventbus: acknowledge Pulsar event: %w", err)
		}
	}
}

func (p *PulsarProvider) producerForUse() (pulsar.Producer, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed.Load() {
		return nil, ErrProviderClosed
	}
	if p.producer != nil {
		return p.producer, nil
	}
	producer, err := p.client.CreateProducer(pulsar.ProducerOptions{Topic: p.cfg.Topic})
	if err != nil {
		return nil, fmt.Errorf("goeventbus: create Pulsar producer: %w", err)
	}
	p.producer = producer
	return producer, nil
}

func (p *PulsarProvider) consumerForUse() (pulsar.Consumer, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed.Load() {
		return nil, ErrProviderClosed
	}
	if p.consumer != nil {
		return p.consumer, nil
	}
	subType := pulsar.Shared
	if p.cfg.SubscriptionType != nil {
		subType = *p.cfg.SubscriptionType
	}
	consumer, err := p.client.Subscribe(pulsar.ConsumerOptions{
		Topic:            p.cfg.Topic,
		SubscriptionName: p.cfg.Subscription,
		Type:             subType,
	})
	if err != nil {
		return nil, fmt.Errorf("goeventbus: create Pulsar consumer: %w", err)
	}
	p.consumer = consumer
	return consumer, nil
}

func (p *PulsarProvider) Close() error {
	if p.closed.Swap(true) {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.consumer != nil {
		p.consumer.Close()
		p.consumer = nil
	}
	if p.producer != nil {
		p.producer.Close()
		p.producer = nil
	}
	return nil
}

var _ Provider = (*PulsarProvider)(nil)
