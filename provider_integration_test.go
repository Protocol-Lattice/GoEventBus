//go:build integration

package GoEventBus

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const integrationTestTimeout = 90 * time.Second

func TestRedisProviderIntegration(t *testing.T) {
	ctx := integrationContext(t)
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "redis:7.4-alpine",
			ExposedPorts: []string{"6379/tcp"},
			WaitingFor:   wait.ForListeningPort("6379/tcp"),
		},
		Started: true,
	})
	testcontainers.CleanupContainer(t, container)
	if err != nil {
		t.Fatalf("start Redis container: %v", err)
	}

	endpoint, err := container.PortEndpoint(ctx, "6379/tcp", "")
	if err != nil {
		t.Fatalf("get Redis endpoint: %v", err)
	}
	client := redis.NewClient(&redis.Options{Addr: endpoint})
	t.Cleanup(func() { _ = client.Close() })

	stream := "goeventbus:integration:redis"
	group := "goeventbus-integration"
	delivered := make(chan Event, 2)
	dispatcher := Dispatcher{}
	dispatcher.Register("orders.created", func(_ context.Context, event Event) (Result, error) {
		delivered <- event
		return Result{}, nil
	})

	store := NewEventStore(
		&dispatcher,
		8,
		DropOldest,
		WithRedis(RedisProviderConfig{
			Client:   client,
			Stream:   stream,
			Group:    group,
			Consumer: "consumer-1",
			StartID:  "0",
			Block:    100 * time.Millisecond,
			Count:    1,
		}),
	)
	consumeErr := runStoreConsumer(t, ctx, store)

	waitFor(t, ctx, "Redis consumer group", func() (bool, error) {
		groups, err := client.XInfoGroups(ctx, stream).Result()
		if err != nil {
			return false, nil
		}
		for _, info := range groups {
			if info.Name == group {
				return true, nil
			}
		}
		return false, nil
	})

	want := Event{
		ID:         "redis-1",
		Projection: "orders.created",
		Data:       map[string]any{"order_id": "o-42"},
	}
	decision, err := store.DecideAndSubscribe(
		ctx,
		fixedEventSelector("order_created"),
		map[string]any{"message": "create order o-42"},
		Event{ID: want.ID, Data: want.Data},
		[]EventCandidate{{
			Key:         "order_created",
			Projection:  want.Projection,
			Description: "A new order should be created",
		}},
	)
	if err != nil {
		t.Fatalf("DecideAndSubscribe Redis event: %v", err)
	}
	if decision.Projection != want.Projection {
		t.Fatalf("decision projection = %#v, want %#v", decision.Projection, want.Projection)
	}

	assertDeliveredEvent(t, receiveEvent(t, ctx, delivered, consumeErr), want)
	waitFor(t, ctx, "Redis acknowledgement", func() (bool, error) {
		pending, err := client.XPending(ctx, stream, group).Result()
		if err != nil {
			return false, err
		}
		return pending.Count == 0, nil
	})
}

func TestRabbitMQProviderIntegration(t *testing.T) {
	ctx := integrationContext(t)
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "rabbitmq:3.13-alpine",
			ExposedPorts: []string{"5672/tcp"},
			Env: map[string]string{
				"RABBITMQ_DEFAULT_USER": "goeventbus",
				"RABBITMQ_DEFAULT_PASS": "goeventbus",
			},
			WaitingFor: wait.ForAll(
				wait.ForListeningPort("5672/tcp"),
				wait.ForLog("Server startup complete"),
			),
		},
		Started: true,
	})
	testcontainers.CleanupContainer(t, container)
	if err != nil {
		t.Fatalf("start RabbitMQ container: %v", err)
	}

	endpoint, err := container.PortEndpoint(ctx, "5672/tcp", "")
	if err != nil {
		t.Fatalf("get RabbitMQ endpoint: %v", err)
	}
	url := fmt.Sprintf("amqp://goeventbus:goeventbus@%s/", endpoint)

	delivered := make(chan Event, 2)
	dispatcher := Dispatcher{}
	dispatcher.Register("orders.created", func(_ context.Context, event Event) (Result, error) {
		delivered <- event
		return Result{}, nil
	})

	store := NewEventStore(
		&dispatcher,
		8,
		DropOldest,
		WithRabbitMQ(RabbitMQProviderConfig{
			URL:           url,
			Exchange:      "goeventbus.integration",
			Queue:         "goeventbus.integration.queue",
			BindingKey:    "orders.*",
			Consumer:      "consumer-1",
			PrefetchCount: 1,
		}),
	)
	consumeErr := runStoreConsumer(t, ctx, store)

	want := []Event{
		{ID: "rabbit-1", Projection: "orders.created", Data: map[string]any{"order_id": "o-42"}},
		{ID: "rabbit-2", Projection: "orders.created", Data: map[string]any{"order_id": "o-43"}},
	}
	for _, event := range want {
		decision, err := store.DecideAndSubscribe(
			ctx,
			fixedEventSelector("order_created"),
			map[string]any{"message": "create " + event.ID},
			Event{ID: event.ID, Data: event.Data},
			[]EventCandidate{{
				Key:         "order_created",
				Projection:  event.Projection,
				Description: "A new order should be created",
			}},
		)
		if err != nil {
			t.Fatalf("DecideAndSubscribe RabbitMQ event %q: %v", event.ID, err)
		}
		if decision.Projection != event.Projection {
			t.Fatalf("decision projection = %#v, want %#v", decision.Projection, event.Projection)
		}
	}

	for _, event := range want {
		assertDeliveredEvent(t, receiveEvent(t, ctx, delivered, consumeErr), event)
	}
}

type fixedEventSelector string

func (s fixedEventSelector) SelectEvent(
	context.Context,
	any,
	[]EventCandidate,
) (EventDecision, error) {
	choice := string(s)
	return EventDecision{
		Choice:        choice,
		Confidence:    1,
		Probabilities: map[string]float64{choice: 1},
	}, nil
}

func integrationContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), integrationTestTimeout)
	t.Cleanup(cancel)
	return ctx
}

func runStoreConsumer(t *testing.T, parent context.Context, store *EventStore) <-chan error {
	t.Helper()
	ctx, cancel := context.WithCancel(parent)
	consumeErr := make(chan error, 1)
	go func() {
		consumeErr <- store.Consume(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		_ = store.Close(context.Background())
		select {
		case err := <-consumeErr:
			if !errors.Is(err, context.Canceled) &&
				!errors.Is(err, context.DeadlineExceeded) &&
				!errors.Is(err, ErrProviderClosed) {
				t.Errorf("Consume returned %v after shutdown", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("Consume did not stop after shutdown")
		}
	})
	return consumeErr
}

func receiveEvent(t *testing.T, ctx context.Context, delivered <-chan Event, consumeErr <-chan error) Event {
	t.Helper()
	select {
	case event := <-delivered:
		return event
	case err := <-consumeErr:
		t.Fatalf("Consume returned before delivery: %v", err)
	case <-ctx.Done():
		t.Fatalf("timed out waiting for delivery: %v", ctx.Err())
	}
	return Event{}
}

func assertDeliveredEvent(t *testing.T, got, want Event) {
	t.Helper()
	if got.ID != want.ID {
		t.Errorf("event ID = %q, want %q", got.ID, want.ID)
	}
	if got.Projection != want.Projection {
		t.Errorf("event projection = %q, want %q", got.Projection, want.Projection)
	}
	if got.Data.(map[string]any)["order_id"] != want.Data.(map[string]any)["order_id"] {
		t.Errorf("event payload = %#v, want %#v", got.Data, want.Data)
	}
}

func waitFor(t *testing.T, ctx context.Context, description string, condition func() (bool, error)) {
	t.Helper()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		ok, err := condition()
		if err != nil {
			t.Fatalf("check %s: %v", description, err)
		}
		if ok {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for %s: %v", description, ctx.Err())
		case <-ticker.C:
		}
	}
}
