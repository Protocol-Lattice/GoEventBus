package GoEventBus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

var (
	// ErrNilProvider is returned when EventStore.Consume receives an explicit nil provider
	// or WithProvider is configured with nil.
	ErrNilProvider = errors.New("goeventbus: provider must not be nil")
	// ErrNoProvider is returned when a provider-backed store operation is used
	// without configuring a provider option.
	ErrNoProvider = errors.New("goeventbus: no provider configured")
	// ErrProviderAlreadyConfigured is returned when more than one provider option
	// is supplied to NewEventStore.
	ErrProviderAlreadyConfigured = errors.New("goeventbus: provider already configured")
	// ErrTooManyProviders is returned when Consume is called with more than one
	// explicit provider. Pass no provider to use the NewEventStore option.
	ErrTooManyProviders = errors.New("goeventbus: Consume accepts at most one provider")
	// ErrNilConsumer is returned when a provider is asked to consume without a callback.
	ErrNilConsumer = errors.New("goeventbus: consumer must not be nil")
	// ErrProviderClosed is returned after a provider has been closed.
	ErrProviderClosed = errors.New("goeventbus: provider is closed")
	// ErrStringProjection is returned when an event cannot be routed by a remote broker.
	ErrStringProjection = errors.New("goeventbus: remote providers require a string projection")
)

// EventConsumer receives an event from a Provider. Returning an error tells
// the provider not to acknowledge the broker message, so it can be retried.
type EventConsumer func(context.Context, Event) error

// Provider publishes events to, and consumes events from, an external broker.
// Consume blocks until ctx is cancelled, the provider is closed, or an error
// occurs. Implementations acknowledge a broker message only after consumer
// returns nil.
type Provider interface {
	Publish(context.Context, Event) error
	Consume(context.Context, EventConsumer) error
	Close() error
}

// EventStoreOption configures optional EventStore integrations.
//
// Provider options are intentionally variadic on NewEventStore so existing
// three-argument construction remains source-compatible.
type EventStoreOption func(*EventStore) error

// WithProvider attaches an already-created provider to the EventStore. The
// store owns the provider and closes it during Drain/Close.
//
// Prefer WithRedis or WithRabbitMQ when the provider should be constructed
// lazily from its config on first use.
func WithProvider(provider Provider) EventStoreOption {
	return func(es *EventStore) error {
		if provider == nil {
			return ErrNilProvider
		}
		es.providerMu.Lock()
		defer es.providerMu.Unlock()
		if es.provider != nil || es.providerFactory != nil {
			return ErrProviderAlreadyConfigured
		}
		es.provider = provider
		return nil
	}
}

func withProviderFactory(factory func() (Provider, error)) EventStoreOption {
	return func(es *EventStore) error {
		if factory == nil {
			return ErrNilProvider
		}
		es.providerMu.Lock()
		defer es.providerMu.Unlock()
		if es.provider != nil || es.providerFactory != nil {
			return ErrProviderAlreadyConfigured
		}
		es.providerFactory = factory
		return nil
	}
}

func (es *EventStore) hasConfiguredProvider() bool {
	es.providerMu.Lock()
	defer es.providerMu.Unlock()
	return es.provider != nil || es.providerFactory != nil
}

func (es *EventStore) providerForUse() (Provider, error) {
	es.providerMu.Lock()
	defer es.providerMu.Unlock()

	if es.closed.Load() {
		return nil, ErrEventStoreClosed
	}
	if es.provider != nil {
		return es.provider, nil
	}
	if es.providerFactory == nil {
		return nil, ErrNoProvider
	}

	provider, err := es.providerFactory()
	if err != nil {
		return nil, err
	}
	if provider == nil {
		return nil, ErrNilProvider
	}
	es.provider = provider
	return provider, nil
}

func (es *EventStore) closeConfiguredProvider() error {
	es.providerMu.Lock()
	provider := es.provider
	es.provider = nil
	es.providerFactory = nil
	es.providerMu.Unlock()

	if provider == nil {
		return nil
	}
	return provider.Close()
}

// PublishToProvider publishes an event through the provider configured on
// NewEventStore. Local Subscribe/Publish semantics are unchanged.
func (es *EventStore) PublishToProvider(ctx context.Context, event Event) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if !es.enterOperation() {
		return ErrEventStoreClosed
	}
	defer es.activeOps.Done()

	provider, err := es.providerForUse()
	if err != nil {
		return err
	}
	return provider.Publish(ctx, event)
}

// EventCodec serializes an Event for transport and reconstructs it on receipt.
// A custom codec is useful when handlers require concrete payload types instead
// of the map[string]any returned by JSONCodec's default decoder.
type EventCodec interface {
	Encode(Event) ([]byte, error)
	Decode([]byte) (Event, error)
}

// JSONCodec is the default transport codec. It serializes ID, Projection,
// Data, and legacy Args; Event.Ctx is intentionally local and is not sent.
// Remote providers require Projection to be a string so it can be used as a
// stable Redis/RabbitMQ routing name.
//
// DecodePayload, when non-nil, converts the JSON payload to an application
// type. Without it, Data is decoded as map[string]any, []any, string, float64,
// bool, or nil according to encoding/json's standard rules.
type JSONCodec struct {
	DecodePayload func(projection string, payload json.RawMessage) (any, error)
}

type transportEvent struct {
	ID         string          `json:"id,omitempty"`
	Projection string          `json:"projection"`
	Data       json.RawMessage `json:"data"`
	Args       map[string]any  `json:"args,omitempty"`
}

// Encode serializes e into the broker-neutral JSON envelope.
func (c JSONCodec) Encode(e Event) ([]byte, error) {
	projection, err := remoteProjection(e)
	if err != nil {
		return nil, err
	}

	payload, err := json.Marshal(e.Data)
	if err != nil {
		return nil, fmt.Errorf("goeventbus: encode event data: %w", err)
	}
	encoded, err := json.Marshal(transportEvent{
		ID:         e.ID,
		Projection: projection,
		Data:       payload,
		Args:       e.Args,
	})
	if err != nil {
		return nil, fmt.Errorf("goeventbus: encode event envelope: %w", err)
	}
	return encoded, nil
}

func remoteProjection(e Event) (string, error) {
	projection, ok := e.Projection.(string)
	if !ok {
		return "", fmt.Errorf("%w: got %T", ErrStringProjection, e.Projection)
	}
	if projection == "" {
		return "", errors.New("goeventbus: remote event projection must not be empty")
	}
	return projection, nil
}

// Decode deserializes a broker-neutral JSON envelope into an Event.
func (c JSONCodec) Decode(encoded []byte) (Event, error) {
	var wire transportEvent
	if err := json.Unmarshal(encoded, &wire); err != nil {
		return Event{}, fmt.Errorf("goeventbus: decode event envelope: %w", err)
	}
	if wire.Projection == "" {
		return Event{}, errors.New("goeventbus: decoded event has an empty projection")
	}

	var (
		data any
		err  error
	)
	if c.DecodePayload != nil {
		data, err = c.DecodePayload(wire.Projection, wire.Data)
		if err != nil {
			return Event{}, fmt.Errorf("goeventbus: decode event data for %q: %w", wire.Projection, err)
		}
	} else if len(wire.Data) > 0 && string(wire.Data) != "null" {
		if err := json.Unmarshal(wire.Data, &data); err != nil {
			return Event{}, fmt.Errorf("goeventbus: decode event data: %w", err)
		}
	}

	return Event{
		ID:         wire.ID,
		Projection: wire.Projection,
		Data:       data,
		Args:       wire.Args,
	}, nil
}

// Consume receives remote events and feeds them into the local EventStore.
//
// With no explicit provider argument, Consume uses the provider configured via
// NewEventStore options. Passing one provider preserves the previous explicit
// provider API. A provider acknowledges an event only after the store accepts
// it. Handler errors continue to follow EventStore's normal error and DLQ
// semantics.
func (es *EventStore) Consume(ctx context.Context, providers ...Provider) error {
	if ctx == nil {
		ctx = context.Background()
	}

	var provider Provider
	switch len(providers) {
	case 0:
		if es.closed.Load() {
			return ErrEventStoreClosed
		}
		configured, err := es.providerForUse()
		if err != nil {
			return err
		}
		provider = configured
	case 1:
		provider = providers[0]
		if provider == nil {
			return ErrNilProvider
		}
	default:
		return ErrTooManyProviders
	}

	return provider.Consume(ctx, func(eventCtx context.Context, event Event) error {
		if eventCtx == nil {
			eventCtx = ctx
		}
		if err := es.Subscribe(eventCtx, event); err != nil {
			return err
		}
		es.Publish()
		return nil
	})
}
