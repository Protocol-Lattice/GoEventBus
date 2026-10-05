package GoEventBus

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/service/sns"
)

type SNSAPI interface {
	Publish(context.Context, *sns.PublishInput, ...func(*sns.Options)) (*sns.PublishOutput, error)
}

type SNSProviderConfig struct {
	Client   SNSAPI
	TopicARN string

	FIFO                   bool
	MessageGroupID         string
	MessageDeduplicationID func(Event) string

	// Subscriber is optional. SNS itself is publish-only; configure an SQS-backed
	// Provider (or another supported subscriber transport) to enable Consume.
	Subscriber Provider
	Codec      EventCodec
}

type SNSProvider struct {
	client SNSAPI
	cfg    SNSProviderConfig
	codec  EventCodec
	closed atomic.Bool
}

func WithSNS(config SNSProviderConfig) EventStoreOption {
	return withProviderFactory(func() (Provider, error) { return NewSNSProvider(config) })
}

func NewSNSProvider(config SNSProviderConfig) (*SNSProvider, error) {
	if config.Client == nil {
		return nil, errors.New("goeventbus: SNS client must not be nil")
	}
	if config.TopicARN == "" {
		return nil, errors.New("goeventbus: SNS topic ARN must not be empty")
	}
	if config.Codec == nil {
		config.Codec = JSONCodec{}
	}
	if config.FIFO && config.MessageGroupID == "" {
		config.MessageGroupID = "goeventbus"
	}
	return &SNSProvider{client: config.Client, cfg: config, codec: config.Codec}, nil
}

func (p *SNSProvider) Publish(ctx context.Context, e Event) error {
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
	message := string(payload)
	input := &sns.PublishInput{TopicArn: stringPtr(p.cfg.TopicARN), Message: &message}
	if p.cfg.FIFO {
		input.MessageGroupId = stringPtr(p.cfg.MessageGroupID)
		if p.cfg.MessageDeduplicationID != nil {
			if id := p.cfg.MessageDeduplicationID(e); id != "" {
				input.MessageDeduplicationId = &id
			}
		} else if e.ID != "" {
			input.MessageDeduplicationId = stringPtr(e.ID)
		}
	}
	if _, err := p.client.Publish(ctx, input); err != nil {
		return fmt.Errorf("goeventbus: publish SNS event: %w", err)
	}
	return nil
}

func (p *SNSProvider) Consume(ctx context.Context, consumer EventConsumer) error {
	if consumer == nil {
		return ErrNilConsumer
	}
	if p.closed.Load() {
		return ErrProviderClosed
	}
	if p.cfg.Subscriber == nil {
		return errors.New("goeventbus: SNS consume requires a subscriber transport such as SQS")
	}
	return p.cfg.Subscriber.Consume(ctx, consumer)
}

func (p *SNSProvider) Close() error {
	if p.closed.Swap(true) {
		return nil
	}
	if p.cfg.Subscriber != nil {
		return p.cfg.Subscriber.Close()
	}
	return nil
}

var _ Provider = (*SNSProvider)(nil)
