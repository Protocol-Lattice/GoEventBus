package GoEventBus

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestContainer(t *testing.T) {
	// Create a context for container operations.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start a RabbitMQ container.
	req := testcontainers.ContainerRequest{
		Image:        "rabbitmq:3",
		ExposedPorts: []string{"5672/tcp"},
		WaitingFor:   wait.ForListeningPort("5672/tcp"),
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("failed to start RabbitMQ container: %v", err)
	}
	defer container.Terminate(ctx)

	// Retrieve connection details.
	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("failed to get container host: %v", err)
	}
	port, err := container.MappedPort(ctx, "5672")
	if err != nil {
		t.Fatalf("failed to get mapped port: %v", err)
	}
	rabbitURL := fmt.Sprintf("amqp://guest:guest@%s:%s/", host, port.Port())

	// Subtest: Commit success.
	t.Run("Commit", func(t *testing.T) {
		var callCount int
		dummyHandler := func(args map[string]interface{}) (Result, error) {
			callCount++
			return Result{Message: "handled"}, nil
		}
		dispatcher := RabbitDispatcher{
			"testProjection": dummyHandler,
		}
		store, err := NewRabbitEventStore(&dispatcher, rabbitURL, "testQueue")
		if err != nil {
			t.Fatalf("failed to create RabbitEventStore: %v", err)
		}
		defer store.RabbitChannel.Close()
		defer store.RabbitConn.Close()

		event := NewEvent("testProjection", map[string]interface{}{"key": "value"})
		store.Publish(event)
		// Allow time for the message to be enqueued.
		time.Sleep(1 * time.Second)
		msg, ok, err := store.RabbitChannel.Get("testQueue", true)
		if err != nil {
			t.Fatalf("failed to get message from queue: %v", err)
		}
		if !ok {
			t.Fatal("expected a message in the queue but found none")
		}
		var receivedEvent Event
		if err = json.Unmarshal(msg.Body, &receivedEvent); err != nil {
			t.Fatalf("failed to unmarshal message: %v", err)
		}
		if err = store.Commit(&receivedEvent); err != nil {
			t.Errorf("Commit failed: %v", err)
		}
		if callCount != 1 {
			t.Errorf("expected handler to be called once, got %d", callCount)
		}
	})

	// Subtest: Broadcast success.
	t.Run("Broadcast", func(t *testing.T) {
		ctxBroadcast, cancelBroadcast := context.WithCancel(ctx)
		defer cancelBroadcast()

		callCount := 0
		done := make(chan struct{})
		var once sync.Once
		dummyHandler := func(args map[string]interface{}) (Result, error) {
			callCount++
			once.Do(func() {
				close(done)
			})
			return Result{Message: "handled"}, nil
		}
		dispatcher := RabbitDispatcher{
			"testProjection": dummyHandler,
		}
		store, err := NewRabbitEventStore(&dispatcher, rabbitURL, "testQueue")
		if err != nil {
			t.Fatalf("failed to create RabbitEventStore: %v", err)
		}
		defer store.RabbitChannel.Close()
		defer store.RabbitConn.Close()

		// Start the Broadcast consumer.
		go store.Broadcast(ctxBroadcast)
		// Wait briefly to ensure the consumer is registered.
		time.Sleep(500 * time.Millisecond)

		event := NewEvent("testProjection", map[string]interface{}{"key": "value"})
		store.Publish(event)

		select {
		case <-done:
			// Event was processed.
		case <-time.After(10 * time.Second):
			t.Fatalf("timeout waiting for event to be processed")
		}
		cancelBroadcast()
		if callCount != 1 {
			t.Errorf("expected handler to be called once, got %d", callCount)
		}
	})

	// Subtest: Publish with a nil event.
	t.Run("Publish_NilEvent", func(t *testing.T) {
		dispatcher := RabbitDispatcher{
			"test": func(args map[string]interface{}) (Result, error) { return Result{Message: "ok"}, nil },
		}
		store, err := NewRabbitEventStore(&dispatcher, rabbitURL, "testQueue")
		if err != nil {
			t.Fatalf("failed to create RabbitEventStore: %v", err)
		}
		defer store.RabbitChannel.Close()
		defer store.RabbitConn.Close()
		// Should log error and return without panicking.
		store.Publish(nil)
	})

	// Subtest: Commit with nil dispatcher.
	t.Run("Commit_NoDispatcher", func(t *testing.T) {
		dispatcher := RabbitDispatcher{
			"testProjection": func(args map[string]interface{}) (Result, error) { return Result{Message: "handled"}, nil },
		}
		store, err := NewRabbitEventStore(&dispatcher, rabbitURL, "testQueue")
		if err != nil {
			t.Fatalf("failed to create RabbitEventStore: %v", err)
		}
		defer store.RabbitChannel.Close()
		defer store.RabbitConn.Close()

		store.Dispatcher = nil
		event := NewEvent("testProjection", map[string]interface{}{"key": "value"})
		err = store.Commit(event)
		if err == nil || err.Error() != "dispatcher is nil" {
			t.Errorf("expected error 'dispatcher is nil', got %v", err)
		}
	})

	// Subtest: Commit with no handler for event's projection.
	t.Run("Commit_NoHandler", func(t *testing.T) {
		dispatcher := RabbitDispatcher{}
		store, err := NewRabbitEventStore(&dispatcher, rabbitURL, "testQueue")
		if err != nil {
			t.Fatalf("failed to create RabbitEventStore: %v", err)
		}
		defer store.RabbitChannel.Close()
		defer store.RabbitConn.Close()

		event := NewEvent("nonexistent", map[string]interface{}{"key": "value"})
		err = store.Commit(event)
		expectedErr := fmt.Sprintf("no handler for event projection: %v", event.Projection)
		if err == nil || err.Error() != expectedErr {
			t.Errorf("expected error '%s', got %v", expectedErr, err)
		}
	})

	// Subtest: Commit when handler returns an error.
	t.Run("Commit_HandlerError", func(t *testing.T) {
		dispatcher := RabbitDispatcher{
			"testProjection": func(args map[string]interface{}) (Result, error) {
				return Result{}, fmt.Errorf("handler error")
			},
		}
		store, err := NewRabbitEventStore(&dispatcher, rabbitURL, "testQueue")
		if err != nil {
			t.Fatalf("failed to create RabbitEventStore: %v", err)
		}
		defer store.RabbitChannel.Close()
		defer store.RabbitConn.Close()

		event := NewEvent("testProjection", map[string]interface{}{"key": "value"})
		err = store.Commit(event)
		if err == nil || err.Error() != "error handling event: handler error" {
			t.Errorf("expected handler error, got %v", err)
		}
	})
}
