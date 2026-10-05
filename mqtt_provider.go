package GoEventBus

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type MQTTProviderConfig struct {
	// Client can be injected. If nil, Options is used to construct and own one.
	Client  mqtt.Client
	Options *mqtt.ClientOptions

	Topic       string
	TopicPrefix string
	QoS         byte
	Retained    bool

	OperationTimeout time.Duration
	Codec            EventCodec
}

type MQTTProvider struct {
	client mqtt.Client
	owned  bool
	cfg    MQTTProviderConfig
	codec  EventCodec

	connectMu sync.Mutex
	closed    atomic.Bool
}

func WithMQTT(config MQTTProviderConfig) EventStoreOption {
	return withProviderFactory(func() (Provider, error) { return NewMQTTProvider(config) })
}

func NewMQTTProvider(config MQTTProviderConfig) (*MQTTProvider, error) {
	if config.Client == nil && config.Options == nil {
		return nil, errors.New("goeventbus: MQTT client or client options are required")
	}
	if config.QoS > 2 {
		return nil, errors.New("goeventbus: MQTT QoS must be 0, 1, or 2")
	}
	if config.Topic == "" && config.TopicPrefix == "" {
		return nil, errors.New("goeventbus: MQTT topic or topic prefix is required")
	}
	if config.OperationTimeout <= 0 {
		config.OperationTimeout = 30 * time.Second
	}
	if config.Codec == nil {
		config.Codec = JSONCodec{}
	}

	client := config.Client
	owned := false
	if client == nil {
		client = mqtt.NewClient(config.Options)
		owned = true
	}
	return &MQTTProvider{client: client, owned: owned, cfg: config, codec: config.Codec}, nil
}

func (p *MQTTProvider) Publish(ctx context.Context, e Event) error {
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
	if err := p.ensureConnected(ctx); err != nil {
		return err
	}
	payload, err := p.codec.Encode(e)
	if err != nil {
		return err
	}
	topic := p.cfg.Topic
	if p.cfg.TopicPrefix != "" {
		topic = p.cfg.TopicPrefix + projection
	}
	token := p.client.Publish(topic, p.cfg.QoS, p.cfg.Retained, payload)
	if err := waitMQTTToken(ctx, token, p.cfg.OperationTimeout); err != nil {
		return fmt.Errorf("goeventbus: publish MQTT event: %w", err)
	}
	return nil
}

func (p *MQTTProvider) Consume(ctx context.Context, consumer EventConsumer) error {
	if consumer == nil {
		return ErrNilConsumer
	}
	if p.closed.Load() {
		return ErrProviderClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := p.ensureConnected(ctx); err != nil {
		return err
	}

	topic := p.cfg.Topic
	if p.cfg.TopicPrefix != "" {
		topic = p.cfg.TopicPrefix + "#"
	}
	if strings.TrimSpace(topic) == "" {
		return errors.New("goeventbus: MQTT subscription topic is empty")
	}

	errCh := make(chan error, 1)
	token := p.client.Subscribe(topic, p.cfg.QoS, func(_ mqtt.Client, msg mqtt.Message) {
		if p.closed.Load() {
			return
		}
		event, err := p.codec.Decode(msg.Payload())
		if err == nil {
			err = consumer(ctx, event)
		}
		if err != nil {
			select {
			case errCh <- err:
			default:
			}
		}
	})
	if err := waitMQTTToken(ctx, token, p.cfg.OperationTimeout); err != nil {
		return fmt.Errorf("goeventbus: subscribe MQTT topic %q: %w", topic, err)
	}
	defer func() {
		if p.client.IsConnectionOpen() {
			_ = waitMQTTToken(context.Background(), p.client.Unsubscribe(topic), p.cfg.OperationTimeout)
		}
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}

func (p *MQTTProvider) ensureConnected(ctx context.Context) error {
	if p.client.IsConnectionOpen() {
		return nil
	}
	p.connectMu.Lock()
	defer p.connectMu.Unlock()
	if p.client.IsConnectionOpen() {
		return nil
	}
	if p.closed.Load() {
		return ErrProviderClosed
	}
	if err := waitMQTTToken(ctx, p.client.Connect(), p.cfg.OperationTimeout); err != nil {
		return fmt.Errorf("goeventbus: connect MQTT client: %w", err)
	}
	return nil
}

func (p *MQTTProvider) Close() error {
	if p.closed.Swap(true) {
		return nil
	}
	if p.owned && p.client.IsConnectionOpen() {
		p.client.Disconnect(250)
	}
	return nil
}

func waitMQTTToken(ctx context.Context, token mqtt.Token, maxWait time.Duration) error {
	if token == nil {
		return errors.New("MQTT operation returned a nil token")
	}
	deadline := time.NewTimer(maxWait)
	defer deadline.Stop()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		if token.WaitTimeout(0) {
			return token.Error()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return errors.New("MQTT operation timed out")
		case <-ticker.C:
		}
	}
}

var _ Provider = (*MQTTProvider)(nil)
