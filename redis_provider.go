package GoEventBus

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

const redisPayloadField = "event"

// RedisProviderConfig configures a Redis Streams provider. Client is injected
// so applications control authentication, TLS, pooling, and whether the
// client is shared with other Redis work.
type RedisProviderConfig struct {
	Client   redis.UniversalClient
	Stream   string
	Group    string
	Consumer string

	// StartID is used when the consumer group is created. It defaults to "$",
	// which starts the new group with events published after its creation.
	StartID string
	// Block controls how long a stream read waits for new messages. It defaults
	// to five seconds so Consume periodically observes cancellation and Close.
	Block time.Duration
	// Count is the maximum number of stream messages fetched per read. It
	// defaults to 10.
	Count int64
	// MaxLen limits stream retention when positive, using Redis's approximate
	// trimming mode. Zero leaves retention under Redis/operator control.
	MaxLen int64
	Codec  EventCodec
}

// RedisProvider publishes Events to Redis Streams and consumes them through a
// Redis consumer group. The injected Redis client is never closed by Provider.
type RedisProvider struct {
	client   redis.UniversalClient
	stream   string
	group    string
	consumer string
	startID  string
	block    time.Duration
	count    int64
	maxLen   int64
	codec    EventCodec
	closed   atomic.Bool
}

// NewRedisProvider creates a Redis Streams provider. It does not make a Redis
// request until Publish or Consume is called.
func NewRedisProvider(config RedisProviderConfig) (*RedisProvider, error) {
	if config.Client == nil {
		return nil, errors.New("goeventbus: redis client must not be nil")
	}
	if config.Stream == "" {
		return nil, errors.New("goeventbus: redis stream must not be empty")
	}
	if config.Group == "" {
		return nil, errors.New("goeventbus: redis consumer group must not be empty")
	}
	if config.Consumer == "" {
		return nil, errors.New("goeventbus: redis consumer name must not be empty")
	}
	if config.StartID == "" {
		config.StartID = "$"
	}
	if config.Block <= 0 {
		config.Block = 5 * time.Second
	}
	if config.Count <= 0 {
		config.Count = 10
	}
	if config.Codec == nil {
		config.Codec = JSONCodec{}
	}

	return &RedisProvider{
		client:   config.Client,
		stream:   config.Stream,
		group:    config.Group,
		consumer: config.Consumer,
		startID:  config.StartID,
		block:    config.Block,
		count:    config.Count,
		maxLen:   config.MaxLen,
		codec:    config.Codec,
	}, nil
}

// Publish appends e to the configured Redis stream.
func (p *RedisProvider) Publish(ctx context.Context, e Event) error {
	if p.closed.Load() {
		return ErrProviderClosed
	}
	if _, err := remoteProjection(e); err != nil {
		return err
	}
	payload, err := p.codec.Encode(e)
	if err != nil {
		return err
	}

	args := &redis.XAddArgs{
		Stream: p.stream,
		Values: map[string]any{redisPayloadField: payload},
	}
	if p.maxLen > 0 {
		args.MaxLen = p.maxLen
		args.Approx = true
	}
	if err := p.client.XAdd(ctx, args).Err(); err != nil {
		return fmt.Errorf("goeventbus: publish Redis event: %w", err)
	}
	return nil
}

// Consume first resumes this consumer's pending messages, then reads fresh
// messages from the configured consumer group. A message is acknowledged only
// after consumer returns nil. If consumer returns an error, the message remains
// pending for a retry by the same consumer on its next Consume call.
func (p *RedisProvider) Consume(ctx context.Context, consumer EventConsumer) error {
	if consumer == nil {
		return ErrNilConsumer
	}
	if p.closed.Load() {
		return ErrProviderClosed
	}
	if err := p.ensureGroup(ctx); err != nil {
		return err
	}
	if err := p.consumePending(ctx, consumer); err != nil {
		return err
	}

	for {
		if p.closed.Load() {
			return ErrProviderClosed
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		streams, err := p.client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    p.group,
			Consumer: p.consumer,
			Streams:  []string{p.stream, ">"},
			Count:    p.count,
			Block:    p.block,
		}).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				continue
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("goeventbus: read Redis stream: %w", err)
		}

		if err := p.consumeStreams(ctx, consumer, streams); err != nil {
			return err
		}
	}
}

// Close marks the provider closed. It intentionally does not close the
// injected Redis client, which may be shared by the application.
func (p *RedisProvider) Close() error {
	p.closed.Store(true)
	return nil
}

func (p *RedisProvider) ensureGroup(ctx context.Context) error {
	err := p.client.XGroupCreateMkStream(ctx, p.stream, p.group, p.startID).Err()
	if err == nil || strings.Contains(err.Error(), "BUSYGROUP") {
		return nil
	}
	return fmt.Errorf("goeventbus: create Redis consumer group: %w", err)
}

func (p *RedisProvider) consumePending(ctx context.Context, consumer EventConsumer) error {
	for {
		if p.closed.Load() {
			return ErrProviderClosed
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		streams, err := p.client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    p.group,
			Consumer: p.consumer,
			Streams:  []string{p.stream, "0"},
			Count:    p.count,
			// A pending-message scan must return immediately when there are no
			// pending entries. go-redis emits BLOCK 0 for its zero value, which
			// would wait indefinitely before Consume reached new messages.
			Block: -1,
		}).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				return nil
			}
			return fmt.Errorf("goeventbus: resume Redis pending messages: %w", err)
		}
		if len(streams) == 0 {
			return nil
		}
		if err := p.consumeStreams(ctx, consumer, streams); err != nil {
			return err
		}
	}
}

func (p *RedisProvider) consumeStreams(ctx context.Context, consumer EventConsumer, streams []redis.XStream) error {
	for _, stream := range streams {
		for _, message := range stream.Messages {
			payload, err := redisMessagePayload(message)
			if err != nil {
				return err
			}
			event, err := p.codec.Decode(payload)
			if err != nil {
				return fmt.Errorf("goeventbus: decode Redis event %s: %w", message.ID, err)
			}
			if err := consumer(ctx, event); err != nil {
				return err
			}
			if err := p.client.XAck(ctx, p.stream, p.group, message.ID).Err(); err != nil {
				return fmt.Errorf("goeventbus: acknowledge Redis event %s: %w", message.ID, err)
			}
		}
	}
	return nil
}

func redisMessagePayload(message redis.XMessage) ([]byte, error) {
	value, ok := message.Values[redisPayloadField]
	if !ok {
		return nil, fmt.Errorf("goeventbus: Redis event %s is missing %q", message.ID, redisPayloadField)
	}
	switch payload := value.(type) {
	case string:
		return []byte(payload), nil
	case []byte:
		return payload, nil
	default:
		return nil, fmt.Errorf("goeventbus: Redis event %s has non-text payload %T", message.ID, value)
	}
}

var _ Provider = (*RedisProvider)(nil)
