package GoEventBus

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestRabbitEventStore_Commit(t *testing.T) {
	ctx := context.Background()
	// Define a container request for RabbitMQ
	req := testcontainers.ContainerRequest{
		Image:        "rabbitmq:3", // or "rabbitmq:3-management" if you need the management UI
		ExposedPorts: []string{"5672/tcp"},
		WaitingFor:   wait.ForListeningPort("5672/tcp"),
	}
	// Start the RabbitMQ container
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("failed to start RabbitMQ container: %v", err)
	}
	defer container.Terminate(ctx)

	// Retrieve host and mapped port for connecting to RabbitMQ
	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("failed to get container host: %v", err)
	}
	port, err := container.MappedPort(ctx, "5672")
	if err != nil {
		t.Fatalf("failed to get mapped port: %v", err)
	}
	rabbitURL := fmt.Sprintf("amqp://guest:guest@%s:%s/", host, port.Port())

	// Set up a dummy dispatcher with a handler that increments callCount
	var callCount int
	dummyHandler := func(args map[string]interface{}) (Result, error) {
		callCount++
		return Result{Message: "handled"}, nil
	}
	dispatcher := RabbitDispatcher{
		"testProjection": dummyHandler,
	}

	// Create a RabbitEventStore using the container's connection details and a queue named "testQueue"
	store, err := NewRabbitEventStore(&dispatcher, rabbitURL, "testQueue")
	if err != nil {
		t.Fatalf("failed to create RabbitEventStore: %v", err)
	}
	defer store.RabbitChannel.Close()
	defer store.RabbitConn.Close()

	// Create and publish an event
	event := NewEvent("testProjection", map[string]interface{}{"key": "value"})
	store.Publish(event)

	// Allow some time for the message to be enqueued
	time.Sleep(1 * time.Second)

	// Retrieve the message from the queue
	msg, ok, err := store.RabbitChannel.Get("testQueue", true)
	if err != nil {
		t.Fatalf("failed to get message from queue: %v", err)
	}
	if !ok {
		t.Fatal("expected a message in the queue but found none")
	}

	var receivedEvent Event
	err = json.Unmarshal(msg.Body, &receivedEvent)
	if err != nil {
		t.Fatalf("failed to unmarshal message: %v", err)
	}

	// Process the retrieved event
	err = store.Commit(&receivedEvent)
	if err != nil {
		t.Errorf("Commit failed: %v", err)
	}
	if callCount != 1 {
		t.Errorf("expected handler to be called once, got %d", callCount)
	}
}
