package GoEventBus

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

func TestJSONCodecRoundTrip(t *testing.T) {
	codec := JSONCodec{}
	encoded, err := codec.Encode(Event{
		ID:         "evt-1",
		Projection: "order.created",
		Data:       map[string]any{"order_id": "o-42", "amount": 12},
		Args:       map[string]any{"legacy": true},
	})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	event, err := codec.Decode(encoded)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if event.ID != "evt-1" || event.Projection != "order.created" {
		t.Fatalf("unexpected event identity: %#v", event)
	}
	data, ok := event.Data.(map[string]any)
	if !ok || data["order_id"] != "o-42" || data["amount"] != float64(12) {
		t.Fatalf("unexpected decoded data: %#v", event.Data)
	}
	if event.Args["legacy"] != true {
		t.Fatalf("unexpected decoded args: %#v", event.Args)
	}
}

func TestJSONCodecUsesCustomPayloadDecoder(t *testing.T) {
	type orderPayload struct {
		OrderID string `json:"order_id"`
	}
	codec := JSONCodec{DecodePayload: func(projection string, payload json.RawMessage) (any, error) {
		if projection != "order.created" {
			t.Fatalf("projection = %q, want order.created", projection)
		}
		var decoded orderPayload
		if err := json.Unmarshal(payload, &decoded); err != nil {
			return nil, err
		}
		return decoded, nil
	}}

	encoded, err := codec.Encode(Event{
		Projection: "order.created",
		Data:       orderPayload{OrderID: "o-42"},
	})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	event, err := codec.Decode(encoded)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if data, ok := event.Data.(orderPayload); !ok || data.OrderID != "o-42" {
		t.Fatalf("unexpected custom payload: %#v", event.Data)
	}
}

func TestJSONCodecRejectsNonStringProjection(t *testing.T) {
	_, err := (JSONCodec{}).Encode(Event{Projection: 42})
	if !errors.Is(err, ErrStringProjection) {
		t.Fatalf("Encode error = %v, want ErrStringProjection", err)
	}
}

func TestJSONCodecRejectsEmptyProjection(t *testing.T) {
	_, err := (JSONCodec{}).Encode(Event{Projection: ""})
	if err == nil {
		t.Fatal("Encode succeeded for an empty projection")
	}
}

func TestEventStoreConsumeFeedsProviderEventsIntoHandlers(t *testing.T) {
	dispatcher := Dispatcher{}
	called := 0
	dispatcher.Register("remote", func(_ context.Context, event Event) (Result, error) {
		if event.ID != "remote-1" {
			t.Fatalf("event ID = %q, want remote-1", event.ID)
		}
		called++
		return Result{}, nil
	})
	store := NewEventStore(&dispatcher, 8, DropOldest)
	provider := &testProvider{event: Event{ID: "remote-1", Projection: "remote"}}

	if err := store.Consume(context.Background(), provider); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if !provider.consumed {
		t.Fatal("provider did not receive a consumer callback")
	}
	if called != 1 {
		t.Fatalf("handler calls = %d, want 1", called)
	}
}

func TestEventStoreConsumeRejectsNilProvider(t *testing.T) {
	store := NewEventStore(&Dispatcher{}, 8, DropOldest)
	if err := store.Consume(context.Background(), nil); !errors.Is(err, ErrNilProvider) {
		t.Fatalf("Consume error = %v, want ErrNilProvider", err)
	}
}

type testProvider struct {
	event    Event
	consumed bool
}

func (p *testProvider) Publish(context.Context, Event) error { return nil }

func (p *testProvider) Consume(ctx context.Context, consumer EventConsumer) error {
	p.consumed = true
	return consumer(ctx, p.event)
}

func (p *testProvider) Close() error { return nil }

var _ Provider = (*testProvider)(nil)
