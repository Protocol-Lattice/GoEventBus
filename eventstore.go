package GoEventBus

import (
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

// NewEventStoreWithRabbitMQ initializes an EventStore with RabbitMQ integration
func NewEventStoreWithRabbitMQ(dispatcher *Dispatcher) *EventStore {
	conn, err := amqp.Dial(rabbitMQURL)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel: %v", err)
	}

	// Declare an exchange
	err = ch.ExchangeDeclare(
		rabbitMQExchange, // name
		"fanout",         // type
		true,             // durable
		false,            // auto-deleted
		false,            // internal
		false,            // no-wait
		nil,              // arguments
	)
	if err != nil {
		log.Fatalf("Failed to declare an exchange: %v", err)
	}

	return &EventStore{
		Mutex:      sync.Mutex{},
		Dispatcher: dispatcher,
		Events: &sync.Pool{
			New: func() interface{} {
				return &Event{} // Return a new Event
			},
		},
		RabbitMQ: conn,
		Channel:  ch,
	}
}

// PublishToRabbitMQ sends an event to RabbitMQ
func (eventstore *EventStore) PublishToRabbitMQ(event *Event) error {
	if event == nil {
		return fmt.Errorf("cannot publish a nil event")
	}

	// Format the event data to publish
	body := fmt.Sprintf("EventID: %s, Args: %v", event.Id, event.Args)
	err := eventstore.Channel.Publish(
		rabbitMQExchange, // exchange
		"",               // routing key
		false,            // mandatory
		false,            // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        []byte(body),
		},
	)
	if err != nil {
		return fmt.Errorf("failed to publish message to RabbitMQ: %w", err)
	}

	// Log successful publishing
	log.Printf("Successfully published event ID: %s to RabbitMQ", event.Id)
	return nil
}

// CloseRabbitMQ cleans up RabbitMQ resources
func (eventstore *EventStore) CloseRabbitMQ() {
	if eventstore.Channel != nil {
		eventstore.Channel.Close()
	}
	if eventstore.RabbitMQ != nil {
		eventstore.RabbitMQ.Close()
	}
}

// BroadcastWithRabbitMq processes and publishes all events in the pool
func (eventstore *EventStore) BroadcastWithRabbitMQ() error {
	eventstore.Mutex.Lock()
	defer eventstore.Mutex.Unlock()

	var lastErr error

	for {
		// Get an event from the pool
		curr := eventstore.Events.Get()
		if curr == nil {
			// Exit if no more events are in the pool
			break
		}

		// Type assert the event
		event, ok := curr.(*Event)
		if !ok || event == nil {
			log.Println("Invalid event retrieved from pool")
			continue
		}

		// Publish the event to RabbitMQ
		err := eventstore.PublishToRabbitMQ(event)
		if err != nil {
			log.Printf("Failed to publish event ID %s: %v", event.Id, err)
			lastErr = err // Record the last error encountered
			continue
		}

		// Optionally, handle the event if there's a dispatcher
		if eventstore.Dispatcher != nil {
			handler, exists := (*eventstore.Dispatcher)[event.Projection]
			if exists {
				_, err := handler(event.Args)
				if err != nil {
					log.Printf("Error handling event ID %s: %v", event.Id, err)
					lastErr = err
				}
			} else {
				log.Printf("No handler found for event projection: %s", event.Projection)
			}
		}
	}

	// Return the last encountered error, if any
	return lastErr
}
