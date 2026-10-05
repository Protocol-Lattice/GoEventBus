package GoEventBus

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
)

// KafkaProviderConfig configures an Apache Kafka provider.
type KafkaProviderConfig struct {
	Brokers []string
	Topic   string
	Group   string

	// Partition pins publishing/consumption to a partition when >= 0.
	// Partition must remain -1 when Group is configured because Kafka assigns
	// partitions to consumer-group members.
	Partition int

	MinBytes int
	MaxBytes int
	MaxWait  time.Duration
	Codec    EventCodec
}

// KafkaProvider publishes through kafka-go Writer and consumes with synchronous
// offset commits after successful event handling.
type KafkaProvider struct {
	writer *kafka.Writer
	reader *kafka.Reader

	topic     string
	partition int
	codec     EventCodec

	closeOnce sync.Once
	closeErr  error
	closed    atomic.Bool
}

// WithKafka configures Apache Kafka as the EventStore provider.
func WithKafka(config KafkaProviderConfig) EventStoreOption {
	return withProviderFactory(func() (Provider, error) {
		return NewKafkaProvider(config)
	})
}

// NewKafkaProvider builds Kafka clients. Network I/O begins on Publish/Consume.
func NewKafkaProvider(config KafkaProviderConfig) (*KafkaProvider, error) {
	if len(config.Brokers) == 0 {
		return nil, errors.New("goeventbus: Kafka brokers must not be empty")
	}
	if config.Topic == "" {
		return nil, errors.New("goeventbus: Kafka topic must not be empty")
	}
	if config.Partition < -1 {
		return nil, errors.New("goeventbus: Kafka partition must be -1 or greater")
	}
	if config.Group != "" && config.Partition >= 0 {
		return nil, errors.New("goeventbus: Kafka partition cannot be fixed when a consumer group is configured")
	}
	if config.MinBytes <= 0 {
		config.MinBytes = 1
	}
	if config.MaxBytes <= 0 {
		config.MaxBytes = 10e6
	}
	if config.MaxWait <= 0 {
		config.MaxWait = time.Second
	}
	if config.Codec == nil {
		config.Codec = JSONCodec{}
	}

	writer := &kafka.Writer{
		Addr:         kafka.TCP(config.Brokers...),
		Topic:        config.Topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		Async:        false,
	}

	readerConfig := kafka.ReaderConfig{
		Brokers:        config.Brokers,
		GroupID:        config.Group,
		Topic:          config.Topic,
		MinBytes:       config.MinBytes,
		MaxBytes:       config.MaxBytes,
		MaxWait:        config.MaxWait,
		CommitInterval: 0,
	}
	if config.Group == "" && config.Partition >= 0 {
		readerConfig.Partition = config.Partition
	}

	return &KafkaProvider{
		writer:    writer,
		reader:    kafka.NewReader(readerConfig),
		topic:     config.Topic,
		partition: config.Partition,
		codec:     config.Codec,
	}, nil
}

// Publish writes one Kafka message. Projection is stored as the message key to
// preserve per-projection ordering when the default hash balancer is used.
func (p *KafkaProvider) Publish(ctx context.Context, e Event) error {
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

	msg := kafka.Message{
		Topic: p.topic,
		Key:   []byte(projection),
		Value: payload,
		Time:  time.Now().UTC(),
	}
	if p.partition >= 0 {
		msg.Partition = p.partition
	}
	if e.ID != "" {
		msg.Headers = append(msg.Headers, kafka.Header{Key: "goeventbus-event-id", Value: []byte(e.ID)})
	}

	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("goeventbus: publish Kafka event: %w", err)
	}
	return nil
}

// Consume fetches one message at a time and commits its offset only after the
// EventConsumer returns nil. Failed handling leaves the offset uncommitted.
func (p *KafkaProvider) Consume(ctx context.Context, consumer EventConsumer) error {
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
		msg, err := p.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if p.closed.Load() {
				return ErrProviderClosed
			}
			return fmt.Errorf("goeventbus: fetch Kafka event: %w", err)
		}

		event, err := p.codec.Decode(msg.Value)
		if err != nil {
			return fmt.Errorf("goeventbus: decode Kafka event at partition %d offset %d: %w", msg.Partition, msg.Offset, err)
		}
		if err := consumer(ctx, event); err != nil {
			return err
		}
		if err := p.reader.CommitMessages(ctx, msg); err != nil {
			return fmt.Errorf("goeventbus: commit Kafka offset %d: %w", msg.Offset, err)
		}
	}
}

// Close closes the owned Kafka reader and writer.
func (p *KafkaProvider) Close() error {
	p.closeOnce.Do(func() {
		p.closed.Store(true)
		p.closeErr = errors.Join(p.reader.Close(), p.writer.Close())
	})
	return p.closeErr
}

var _ Provider = (*KafkaProvider)(nil)
