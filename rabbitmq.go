package GoEventBus

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"

	"github.com/streadway/amqp"
)

// Dispatcher maps string representations of projections to handler functions
type RabbitDispatcher map[string]func(map[string]interface{}) (Result, error)

// RabbitEventStore handles publishing and dispatching events via RabbitMQ
type RabbitEventStore struct {
	Mutex         sync.Mutex
	Dispatcher    *RabbitDispatcher
	RabbitConn    *amqp.Connection
	RabbitChannel *amqp.Channel
	QueueName     string
}

// NewRabbitEventStore initializes a RabbitEventStore
func NewRabbitEventStore(dispatcher *RabbitDispatcher, rabbitURL, queueName string) (*RabbitEventStore, error) {
	conn, err := amqp.Dial(rabbitURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to create a channel: %w", err)
	}

	_, err = ch.QueueDeclare(
		queueName,
		true,  // Durable
		false, // Delete when unused
		false, // Exclusive
		false, // No-wait
		nil,   // Arguments
	)
	if err != nil {
		return nil, fmt.Errorf("failed to declare a queue: %w", err)
	}

	return &RabbitEventStore{
		Dispatcher:    dispatcher,
		RabbitConn:    conn,
		RabbitChannel: ch,
		QueueName:     queueName,
	}, nil
}

// Publish sends an event to the RabbitMQ queue
func (store *RabbitEventStore) Publish(event *Event) {
	if event == nil {
		log.Println("Attempted to publish a nil event")
		return
	}

	body, err := json.Marshal(event)
	if err != nil {
		log.Printf("Error serializing event: %v", err)
		return
	}

	err = store.RabbitChannel.Publish(
		"",              // Exchange
		store.QueueName, // Routing key
		false,           // Mandatory
		false,           // Immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		},
	)
	if err != nil {
		log.Printf("Error publishing event to RabbitMQ: %v", err)
	}
}

// Commit processes a single event
func (store *RabbitEventStore) Commit(event *Event) error {
	if store.Dispatcher == nil {
		return fmt.Errorf("dispatcher is nil")
	}

	// Convert Projection to a string for use as a map key
	projectionKey := fmt.Sprintf("%v", event.Projection)
	handler, exists := (*store.Dispatcher)[projectionKey]
	if !exists {
		return fmt.Errorf("no handler for event projection: %v", event.Projection)
	}

	result, err := handler(event.Args)
	if err != nil {
		return fmt.Errorf("error handling event: %w", err)
	}

	log.Printf("Event id %s was successfully processed with result of %v", event.Id, result)
	return nil
}

// Broadcast starts consuming events from RabbitMQ
func (store *RabbitEventStore) Broadcast(ctx context.Context) {
	msgs, err := store.RabbitChannel.Consume(
		store.QueueName,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to consume messages: %v", err)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-msgs:
			if !ok {
				return
			}
			event := &Event{}
			if err := json.Unmarshal(msg.Body, event); err != nil {
				log.Printf("Error deserializing event: %v", err)
				continue
			}
			if err := store.Commit(event); err != nil {
				log.Printf("Error committing event: %v", err)
			}
		}
	}
}
