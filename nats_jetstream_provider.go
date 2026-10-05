package GoEventBus

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
)

// NATSJetStreamProviderConfig configures a NATS JetStream provider.
//
// Conn is injected and remains owned by the caller. The provider creates
// JetStream subscriptions lazily in Consume and never closes Conn.
type NATSJetStreamProviderConfig struct {
	Conn *nats.Conn

	// Subject is the subscription subject. It defaults to SubjectPrefix + ">".
	Subject string
	// SubjectPrefix is prepended to the event projection on publish.
	// It defaults to "events.".
	SubjectPrefix string
	// Durable configures a durable JetStream consumer.
	Durable string
	// Queue enables queue-group consumption for multiple consumers.
	Queue string

	Codec EventCodec
}

// NATSJetStreamProvider publishes to and consumes from NATS JetStream.
// Messages are acknowledged only after EventConsumer succeeds.
type NATSJetStreamProvider struct {
	conn          *nats.Conn
	js            nats.JetStreamContext
	subject       string
	subjectPrefix string
	durable       string
	queue         string
	codec         EventCodec

	mu     sync.Mutex
	sub    *nats.Subscription
	closed atomic.Bool
}

// WithNATSJetStream configures NATS JetStream as the EventStore provider.
func WithNATSJetStream(config NATSJetStreamProviderConfig) EventStoreOption {
	return withProviderFactory(func() (Provider, error) {
		return NewNATSJetStreamProvider(config)
	})
}

// NewNATSJetStreamProvider creates a NATS JetStream provider without taking
// ownership of the injected connection.
func NewNATSJetStreamProvider(config NATSJetStreamProviderConfig) (*NATSJetStreamProvider, error) {
	if config.Conn == nil {
		return nil, errors.New("goeventbus: NATS connection must not be nil")
	}
	if config.SubjectPrefix == "" {
		config.SubjectPrefix = "events."
	}
	if config.Subject == "" {
		config.Subject = config.SubjectPrefix + ">"
	}
	if config.Codec == nil {
		config.Codec = JSONCodec{}
	}

	js, err := config.Conn.JetStream()
	if err != nil {
		return nil, fmt.Errorf("goeventbus: create NATS JetStream context: %w", err)
	}

	return &NATSJetStreamProvider{
		conn:          config.Conn,
		js:            js,
		subject:       config.Subject,
		subjectPrefix: config.SubjectPrefix,
		durable:       config.Durable,
		queue:         config.Queue,
		codec:         config.Codec,
	}, nil
}

// Publish publishes an event to SubjectPrefix + Projection.
func (p *NATSJetStreamProvider) Publish(ctx context.Context, e Event) error {
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

	msg := &nats.Msg{
		Subject: p.subjectPrefix + projection,
		Data:    payload,
		Header:  nats.Header{},
	}
	if e.ID != "" {
		msg.Header.Set(nats.MsgIdHdr, e.ID)
	}

	if _, err := p.js.PublishMsg(msg, nats.Context(ctx)); err != nil {
		return fmt.Errorf("goeventbus: publish NATS JetStream event: %w", err)
	}
	return nil
}

// Consume subscribes with explicit acknowledgements. Handler failures NAK the
// message so JetStream can redeliver it.
func (p *NATSJetStreamProvider) Consume(ctx context.Context, consumer EventConsumer) error {
	if consumer == nil {
		return ErrNilConsumer
	}
	if p.closed.Load() {
		return ErrProviderClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}

	sub, err := p.subscribe()
	if err != nil {
		return err
	}
	defer func() {
		p.mu.Lock()
		if p.sub == sub {
			p.sub = nil
		}
		p.mu.Unlock()
		_ = sub.Unsubscribe()
	}()

	for {
		if p.closed.Load() {
			return ErrProviderClosed
		}
		msg, err := sub.NextMsgWithContext(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return ctx.Err()
			}
			if p.closed.Load() {
				return ErrProviderClosed
			}
			return fmt.Errorf("goeventbus: consume NATS JetStream event: %w", err)
		}

		event, err := p.codec.Decode(msg.Data)
		if err != nil {
			_ = msg.Nak()
			return fmt.Errorf("goeventbus: decode NATS JetStream event: %w", err)
		}
		if err := consumer(ctx, event); err != nil {
			_ = msg.Nak()
			return err
		}
		if err := msg.Ack(); err != nil {
			return fmt.Errorf("goeventbus: acknowledge NATS JetStream event: %w", err)
		}
	}
}

func (p *NATSJetStreamProvider) subscribe() (*nats.Subscription, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed.Load() {
		return nil, ErrProviderClosed
	}
	if p.sub != nil && p.sub.IsValid() {
		return nil, errors.New("goeventbus: NATS JetStream provider already has an active consumer")
	}

	opts := []nats.SubOpt{nats.ManualAck(), nats.AckExplicit()}
	if p.durable != "" {
		opts = append(opts, nats.Durable(p.durable))
	}

	var (
		sub *nats.Subscription
		err error
	)
	if strings.TrimSpace(p.queue) != "" {
		sub, err = p.js.QueueSubscribeSync(p.subject, p.queue, opts...)
	} else {
		sub, err = p.js.SubscribeSync(p.subject, opts...)
	}
	if err != nil {
		return nil, fmt.Errorf("goeventbus: subscribe NATS JetStream subject %q: %w", p.subject, err)
	}
	p.sub = sub
	return sub, nil
}

// Close stops active consumption. The injected NATS connection is not closed.
func (p *NATSJetStreamProvider) Close() error {
	if p.closed.Swap(true) {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.sub == nil {
		return nil
	}
	err := p.sub.Unsubscribe()
	p.sub = nil
	if err != nil && !errors.Is(err, nats.ErrBadSubscription) {
		return fmt.Errorf("goeventbus: close NATS JetStream subscription: %w", err)
	}
	return nil
}

// keep time imported for compatibility with nats context implementations that
// surface timeout errors through NextMsgWithContext on older clients.
var _ = time.Second

var _ Provider = (*NATSJetStreamProvider)(nil)
