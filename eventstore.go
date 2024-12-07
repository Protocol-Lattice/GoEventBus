package GoEventBus

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"

	"github.com/streadway/amqp"
)

// RabbitMQ settings
const (
	rabbitMQURL      = "amqp://guest:guest@localhost:5672/"
	rabbitMQExchange = "events_exchange"
)

// EventStore handles publishing and dispatching events
type EventStore struct {
	Mutex      sync.Mutex
	Dispatcher *Dispatcher
	Events     *sync.Pool
	RabbitMQ   *amqp.Connection
	Channel    *amqp.Channel
}

func (eventstore *EventStore) InitRabbitMQ() error {
	var err error
	eventstore.RabbitMQ, err = amqp.Dial(rabbitMQURL)
	if err != nil {
		return fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	eventstore.Channel, err = eventstore.RabbitMQ.Channel()
	if err != nil {
		return fmt.Errorf("failed to open RabbitMQ channel: %w", err)
	}

	err = eventstore.Channel.ExchangeDeclare(
		rabbitMQExchange,
		"fanout", // Exchange type
		true,     // Durable
		false,    // Auto-deleted
		false,    // Internal
		false,    // No-wait
		nil,      // Arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare exchange: %w", err)
	}

	return nil
}

// NewEventStore initializes an EventStore with a dispatcher and an event pool
func NewEventStore(dispatcher *Dispatcher) *EventStore {
	return &EventStore{
		Mutex:      sync.Mutex{},
		Dispatcher: dispatcher,
		Events: &sync.Pool{
			New: func() interface{} {
				return &Event{} // Return a new, non-nil Event instance
			},
		},
	}
}

// Publish adds an event to the event pool
func (eventstore *EventStore) Publish(event *Event) {
	if event == nil {
		log.Println("Attempted to publish a nil event")
		return
	}
	eventstore.Events.Put(event)
}

// Commit retrieves and processes an event from the pool
func (eventstore *EventStore) Commit() error {
	curr := eventstore.Events.Get()
	if curr == nil {
		return fmt.Errorf("no events to process")
	}

	event, ok := curr.(*Event)
	if !ok || event == nil {
		return fmt.Errorf("invalid event type")
	}

	if eventstore.Dispatcher == nil {
		return fmt.Errorf("dispatcher is nil")
	}

	// Check if the dispatcher has a handler for this event
	handler, exists := (*eventstore.Dispatcher)[event.Projection]
	if !exists {
		return fmt.Errorf("no handler for event projection: %s", event.Projection)
	}

	// Execute the handler
	_, err := handler(event.Args)
	if err != nil {
		return fmt.Errorf("error handling event: %w", err)
	}

	log.Printf("Event id: %s was successfully processed", event.Id)
	return nil
}

// Broadcast locks the store and processes each event in the pool
func (eventstore *EventStore) Broadcast() error {
	eventstore.Mutex.Lock()
	defer eventstore.Mutex.Unlock()

	var lastErr error
	for {
		err := eventstore.Commit()
		if err != nil {
			if err.Error() == "no events to process" {
				break
			}
			lastErr = err
			log.Printf("Error processing event: %v", err)
		}
	}

	return lastErr
}

func (eventstore *EventStore) PublishWithRabbitMQ(event *Event) {
	if event == nil {
		log.Println("Attempted to publish a nil event")
		return
	}

	// Serialize the event (e.g., JSON)
	eventData, err := json.Marshal(event)
	if err != nil {
		log.Printf("Failed to serialize event: %v", err)
		return
	}

	err = eventstore.Channel.Publish(
		rabbitMQExchange, // Exchange
		"",               // Routing key (empty for fanout exchange)
		false,            // Mandatory
		false,            // Immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        eventData,
		},
	)
	if err != nil {
		log.Printf("Failed to publish event to RabbitMQ: %v", err)
		return
	}

	log.Printf("Event %s published to RabbitMQ", event.Id)
	eventstore.Events.Put(event)
}

func (eventstore *EventStore) BroadcastWithRabbitMQ(queueName string) error {
	q, err := eventstore.Channel.QueueDeclare(
		queueName, // Queue name
		true,      // Durable
		false,     // Delete when unused
		false,     // Exclusive
		false,     // No-wait
		nil,       // Arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare queue: %w", err)
	}

	err = eventstore.Channel.QueueBind(
		q.Name,           // Queue name
		"",               // Routing key
		rabbitMQExchange, // Exchange
		false,            // No-wait
		nil,              // Arguments
	)
	if err != nil {
		return fmt.Errorf("failed to bind queue: %w", err)
	}

	messages, err := eventstore.Channel.Consume(
		q.Name, // Queue name
		"",     // Consumer tag
		true,   // Auto-ack
		false,  // Exclusive
		false,  // No-local
		false,  // No-wait
		nil,    // Arguments
	)
	if err != nil {
		return fmt.Errorf("failed to register consumer: %w", err)
	}

	go func() {
		for msg := range messages {
			var event Event
			err := json.Unmarshal(msg.Body, &event)
			if err != nil {
				log.Printf("Failed to deserialize event: %v", err)
				continue
			}

			// Dispatch the event
			handler, exists := (*eventstore.Dispatcher)[event.Projection]
			if !exists {
				log.Printf("no handler for event projection: %s", event.Projection)
				continue
			}

			// Execute the handler
			_, err = handler(event.Args)
			if err != nil {
				continue
			}
		}
	}()

	return nil
}

func (eventstore *EventStore) CloseRabbitMQ() {
	if eventstore.Channel != nil {
		eventstore.Channel.Close()
	}
	if eventstore.RabbitMQ != nil {
		eventstore.RabbitMQ.Close()
	}
}
