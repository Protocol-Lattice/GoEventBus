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
	provider, err := NewRedisProvider(RedisProviderConfig{
		Client:   client,
		Stream:   stream,
		Group:    group,
		Consumer: "consumer-1",
		StartID:  "0",
		Block:    100 * time.Millisecond,
		Count:    1,
	})
	if err != nil {
		t.Fatalf("create Redis provider: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	want := Event{
		ID:         "redis-1",
		Projection: "orders.created",
		Data:       map[string]any{"order_id": "o-42"},
	}
	if err := provider.Publish(ctx, want); err != nil {
		t.Fatalf("publish Redis event: %v", err)
	}

	delivered, consumeErr := consumeProvider(t, ctx, provider)
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
	provider, err := NewRabbitMQProvider(RabbitMQProviderConfig{
		URL:           url,
		Exchange:      "goeventbus.integration",
		Queue:         "goeventbus.integration.queue",
		BindingKey:    "orders.*",
		Consumer:      "consumer-1",
		PrefetchCount: 1,
	})
	if err != nil {
		t.Fatalf("create RabbitMQ provider: %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })

	want := []Event{
		{ID: "rabbit-1", Projection: "orders.created", Data: map[string]any{"order_id": "o-42"}},
		{ID: "rabbit-2", Projection: "orders.created", Data: map[string]any{"order_id": "o-43"}},
	}
	for _, event := range want {
		if err := provider.Publish(ctx, event); err != nil {
			t.Fatalf("publish RabbitMQ event %q: %v", event.ID, err)
		}
	}

	delivered, consumeErr := consumeProvider(t, ctx, provider)
	for _, event := range want {
		assertDeliveredEvent(t, receiveEvent(t, ctx, delivered, consumeErr), event)
	}
}

func integrationContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), integrationTestTimeout)
	t.Cleanup(cancel)
	return ctx
}

func consumeProvider(t *testing.T, parent context.Context, provider Provider) (<-chan Event, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(parent)
	delivered := make(chan Event, 2)
	consumeErr := make(chan error, 1)
	go func() {
		consumeErr <- provider.Consume(ctx, func(_ context.Context, event Event) error {
			delivered <- event
			return nil
		})
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-consumeErr:
			if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("Consume returned %v after cancellation, want context cancellation", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("Consume did not stop after cancellation")
		}
	})
	return delivered, consumeErr
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
