package GoEventBus

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

type AzureServiceBusProviderConfig struct {
	Client *azservicebus.Client

	// Configure Queue for queue semantics, or Topic for topic publishing.
	// Topic consumption additionally requires Subscription.
	Queue        string
	Topic        string
	Subscription string

	Codec EventCodec
}

type AzureServiceBusProvider struct {
	client   *azservicebus.Client
	sender   *azservicebus.Sender
	receiver *azservicebus.Receiver
	codec    EventCodec

	closeMu sync.Mutex
	closed  atomic.Bool
}

func WithAzureServiceBus(config AzureServiceBusProviderConfig) EventStoreOption {
	return withProviderFactory(func() (Provider, error) { return NewAzureServiceBusProvider(config) })
}

func NewAzureServiceBusProvider(config AzureServiceBusProviderConfig) (*AzureServiceBusProvider, error) {
	if config.Client == nil {
		return nil, errors.New("goeventbus: Azure Service Bus client must not be nil")
	}
	if config.Queue == "" && config.Topic == "" {
		return nil, errors.New("goeventbus: Azure Service Bus queue or topic is required")
	}
	if config.Queue != "" && config.Topic != "" {
		return nil, errors.New("goeventbus: Azure Service Bus queue and topic are mutually exclusive")
	}
	if config.Codec == nil {
		config.Codec = JSONCodec{}
	}

	entity := config.Queue
	if entity == "" {
		entity = config.Topic
	}
	sender, err := config.Client.NewSender(entity, nil)
	if err != nil {
		return nil, fmt.Errorf("goeventbus: create Azure Service Bus sender: %w", err)
	}

	var receiver *azservicebus.Receiver
	if config.Queue != "" {
		receiver, err = config.Client.NewReceiverForQueue(config.Queue, &azservicebus.ReceiverOptions{
			ReceiveMode: azservicebus.ReceiveModePeekLock,
		})
	} else if config.Subscription != "" {
		receiver, err = config.Client.NewReceiverForSubscription(config.Topic, config.Subscription, &azservicebus.ReceiverOptions{
			ReceiveMode: azservicebus.ReceiveModePeekLock,
		})
	}
	if err != nil {
		_ = sender.Close(context.Background())
		return nil, fmt.Errorf("goeventbus: create Azure Service Bus receiver: %w", err)
	}

	return &AzureServiceBusProvider{
		client:   config.Client,
		sender:   sender,
		receiver: receiver,
		codec:    config.Codec,
	}, nil
}

func (p *AzureServiceBusProvider) Publish(ctx context.Context, e Event) error {
	if p.closed.Load() {
		return ErrProviderClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if _, err := remoteProjection(e); err != nil {
		return err
	}
	payload, err := p.codec.Encode(e)
	if err != nil {
		return err
	}
	if err := p.sender.SendMessage(ctx, &azservicebus.Message{Body: payload}, nil); err != nil {
		return fmt.Errorf("goeventbus: publish Azure Service Bus event: %w", err)
	}
	return nil
}

func (p *AzureServiceBusProvider) Consume(ctx context.Context, consumer EventConsumer) error {
	if consumer == nil {
		return ErrNilConsumer
	}
	if p.closed.Load() {
		return ErrProviderClosed
	}
	if p.receiver == nil {
		return errors.New("goeventbus: Azure Service Bus receiver is not configured")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	for {
		if p.closed.Load() {
			return ErrProviderClosed
		}
		messages, err := p.receiver.ReceiveMessages(ctx, 1, nil)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if p.closed.Load() {
				return ErrProviderClosed
			}
			return fmt.Errorf("goeventbus: receive Azure Service Bus event: %w", err)
		}
		for _, msg := range messages {
			event, err := p.codec.Decode(msg.Body)
			if err != nil {
				_ = p.receiver.AbandonMessage(ctx, msg, nil)
				return fmt.Errorf("goeventbus: decode Azure Service Bus event: %w", err)
			}
			if err := consumer(ctx, event); err != nil {
				_ = p.receiver.AbandonMessage(ctx, msg, nil)
				return err
			}
			if err := p.receiver.CompleteMessage(ctx, msg, nil); err != nil {
				return fmt.Errorf("goeventbus: complete Azure Service Bus event: %w", err)
			}
		}
	}
}

func (p *AzureServiceBusProvider) Close() error {
	if p.closed.Swap(true) {
		return nil
	}
	p.closeMu.Lock()
	defer p.closeMu.Unlock()
	ctx := context.Background()
	var errs []error
	if p.receiver != nil {
		errs = append(errs, p.receiver.Close(ctx))
	}
	if p.sender != nil {
		errs = append(errs, p.sender.Close(ctx))
	}
	return errors.Join(errs...)
}

var _ Provider = (*AzureServiceBusProvider)(nil)
