package GoEventBus

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/nsqio/go-nsq"
)

type NSQProviderConfig struct {
	NSQDAddress      string
	LookupdAddresses []string
	Topic            string
	Channel          string
	Config           *nsq.Config
	Codec            EventCodec
}

type NSQProvider struct {
	cfg   NSQProviderConfig
	codec EventCodec

	mu       sync.Mutex
	producer *nsq.Producer
	consumer *nsq.Consumer
	closed   atomic.Bool
}

func WithNSQ(config NSQProviderConfig) EventStoreOption {
	return withProviderFactory(func() (Provider, error) { return NewNSQProvider(config) })
}

func NewNSQProvider(config NSQProviderConfig) (*NSQProvider, error) {
	if config.NSQDAddress == "" {
		return nil, errors.New("goeventbus: NSQ nsqd address must not be empty")
	}
	if config.Topic == "" {
		return nil, errors.New("goeventbus: NSQ topic must not be empty")
	}
	if config.Config == nil {
		config.Config = nsq.NewConfig()
	}
	if config.Codec == nil {
		config.Codec = JSONCodec{}
	}
	return &NSQProvider{cfg: config, codec: config.Codec}, nil
}

func (p *NSQProvider) Publish(ctx context.Context, e Event) error {
	if p.closed.Load() {
		return ErrProviderClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := remoteProjection(e); err != nil {
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
	if err := producer.Publish(p.cfg.Topic, payload); err != nil {
		return fmt.Errorf("goeventbus: publish NSQ event: %w", err)
	}
	return nil
}

func (p *NSQProvider) Consume(ctx context.Context, consumer EventConsumer) error {
	if consumer == nil {
		return ErrNilConsumer
	}
	if p.closed.Load() {
		return ErrProviderClosed
	}
	if p.cfg.Channel == "" {
		return errors.New("goeventbus: NSQ channel is required to consume events")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	nc, err := nsq.NewConsumer(p.cfg.Topic, p.cfg.Channel, p.cfg.Config)
	if err != nil {
		return fmt.Errorf("goeventbus: create NSQ consumer: %w", err)
	}
	nc.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
		event, err := p.codec.Decode(message.Body)
		if err != nil {
			return fmt.Errorf("goeventbus: decode NSQ event: %w", err)
		}
		return consumer(ctx, event)
	}))

	p.mu.Lock()
	if p.closed.Load() {
		p.mu.Unlock()
		nc.Stop()
		return ErrProviderClosed
	}
	if p.consumer != nil {
		p.mu.Unlock()
		nc.Stop()
		return errors.New("goeventbus: NSQ provider already has an active consumer")
	}
	p.consumer = nc
	p.mu.Unlock()

	if len(p.cfg.LookupdAddresses) > 0 {
		err = nc.ConnectToNSQLookupds(p.cfg.LookupdAddresses)
	} else {
		err = nc.ConnectToNSQD(p.cfg.NSQDAddress)
	}
	if err != nil {
		p.clearNSQConsumer(nc)
		nc.Stop()
		return fmt.Errorf("goeventbus: connect NSQ consumer: %w", err)
	}

	select {
	case <-ctx.Done():
		nc.Stop()
		<-nc.StopChan
		p.clearNSQConsumer(nc)
		return ctx.Err()
	case <-nc.StopChan:
		p.clearNSQConsumer(nc)
		if p.closed.Load() {
			return ErrProviderClosed
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return errors.New("goeventbus: NSQ consumer stopped")
	}
}

func (p *NSQProvider) producerForUse() (*nsq.Producer, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed.Load() {
		return nil, ErrProviderClosed
	}
	if p.producer != nil {
		return p.producer, nil
	}
	producer, err := nsq.NewProducer(p.cfg.NSQDAddress, p.cfg.Config)
	if err != nil {
		return nil, fmt.Errorf("goeventbus: create NSQ producer: %w", err)
	}
	p.producer = producer
	return producer, nil
}

func (p *NSQProvider) clearNSQConsumer(consumer *nsq.Consumer) {
	p.mu.Lock()
	if p.consumer == consumer {
		p.consumer = nil
	}
	p.mu.Unlock()
}

func (p *NSQProvider) Close() error {
	if p.closed.Swap(true) {
		return nil
	}
	p.mu.Lock()
	consumer := p.consumer
	producer := p.producer
	p.consumer = nil
	p.producer = nil
	p.mu.Unlock()
	if consumer != nil {
		consumer.Stop()
	}
	if producer != nil {
		producer.Stop()
	}
	return nil
}

var _ Provider = (*NSQProvider)(nil)
