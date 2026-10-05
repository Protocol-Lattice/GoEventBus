package GoEventBus

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

type SQSAPI interface {
	SendMessage(context.Context, *sqs.SendMessageInput, ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
	ReceiveMessage(context.Context, *sqs.ReceiveMessageInput, ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(context.Context, *sqs.DeleteMessageInput, ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
}

type SQSProviderConfig struct {
	Client   SQSAPI
	QueueURL string

	WaitTimeSeconds   int32
	VisibilityTimeout int32
	MaxMessages       int32

	FIFO                   bool
	MessageGroupID         string
	MessageDeduplicationID func(Event) string

	Codec EventCodec
}

type SQSProvider struct {
	client SQSAPI
	cfg    SQSProviderConfig
	codec  EventCodec
	closed atomic.Bool
}

func WithSQS(config SQSProviderConfig) EventStoreOption {
	return withProviderFactory(func() (Provider, error) { return NewSQSProvider(config) })
}

func NewSQSProvider(config SQSProviderConfig) (*SQSProvider, error) {
	if config.Client == nil {
		return nil, errors.New("goeventbus: SQS client must not be nil")
	}
	if config.QueueURL == "" {
		return nil, errors.New("goeventbus: SQS queue URL must not be empty")
	}
	if config.WaitTimeSeconds <= 0 {
		config.WaitTimeSeconds = 20
	}
	if config.WaitTimeSeconds > 20 {
		config.WaitTimeSeconds = 20
	}
	if config.MaxMessages <= 0 {
		config.MaxMessages = 10
	}
	if config.MaxMessages > 10 {
		config.MaxMessages = 10
	}
	if config.Codec == nil {
		config.Codec = JSONCodec{}
	}
	if config.FIFO && config.MessageGroupID == "" {
		config.MessageGroupID = "goeventbus"
	}
	return &SQSProvider{client: config.Client, cfg: config, codec: config.Codec}, nil
}

func (p *SQSProvider) Publish(ctx context.Context, e Event) error {
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
	body := string(payload)
	input := &sqs.SendMessageInput{
		QueueUrl:    stringPtr(p.cfg.QueueURL),
		MessageBody: &body,
	}
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
	if _, err := p.client.SendMessage(ctx, input); err != nil {
		return fmt.Errorf("goeventbus: publish SQS event: %w", err)
	}
	return nil
}

func (p *SQSProvider) Consume(ctx context.Context, consumer EventConsumer) error {
	if consumer == nil {
		return ErrNilConsumer
	}
	if p.closed.Load() {
		return ErrProviderClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}

	for {
		if p.closed.Load() {
			return ErrProviderClosed
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		out, err := p.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            stringPtr(p.cfg.QueueURL),
			MaxNumberOfMessages: p.cfg.MaxMessages,
			WaitTimeSeconds:     p.cfg.WaitTimeSeconds,
			VisibilityTimeout:   p.cfg.VisibilityTimeout,
		})
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("goeventbus: receive SQS events: %w", err)
		}
		for _, msg := range out.Messages {
			if msg.Body == nil || msg.ReceiptHandle == nil {
				continue
			}
			event, err := p.codec.Decode([]byte(*msg.Body))
			if err != nil {
				return fmt.Errorf("goeventbus: decode SQS event: %w", err)
			}
			if err := consumer(ctx, event); err != nil {
				return err
			}
			if _, err := p.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
				QueueUrl:      stringPtr(p.cfg.QueueURL),
				ReceiptHandle: msg.ReceiptHandle,
			}); err != nil {
				return fmt.Errorf("goeventbus: delete SQS event after successful handling: %w", err)
			}
		}
		if len(out.Messages) == 0 && p.cfg.WaitTimeSeconds == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(100 * time.Millisecond):
			}
		}
	}
}

func (p *SQSProvider) Close() error {
	p.closed.Store(true)
	return nil
}

func stringPtr(v string) *string { return &v }

var _ Provider = (*SQSProvider)(nil)
