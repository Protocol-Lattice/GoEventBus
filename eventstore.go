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

	log.Printf("Event id: %s was successfully published", event.Id)

	return nil
}

// Broadcast locks the store and processes each event in the pool
func (eventstore *EventStore) Broadcast() error {
	eventstore.Mutex.Lock()
	defer eventstore.Mutex.Unlock()

	var lastErr error
	// Try to commit an event
	for {
		err := eventstore.Commit()
		if err != nil {
			// If there are no more events to process, break the loop
			if err.Error() != "" {
				break
			}
			// Capture the last error if something else goes wrong
			lastErr = err
		}

	}

	return lastErr
}

// NewEventStore initializes an EventStore with a dispatcher and an event pool
func NewEventStoreWithRabbitMq(dispatcher *Dispatcher) *EventStore {
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
	body := fmt.Sprintf("EventID: %v, Args: %v", event.Id, event.Args)
	err := eventstore.Channel.Publish(
		rabbitMQExchange, // exchange
		"",               // routing key
		false,            // mandatory
		false,            // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		},
	)
	if err != nil {
		log.Printf("Failed to publish a message: %v", err)
	}
	handler, exists := (*eventstore.Dispatcher)[event.Projection]
	if !exists {
		return fmt.Errorf("no handler for event projection: %s", event.Projection)
	}

	// Execute the handler
	_, err = handler(event.Args)
	if err != nil {
		return fmt.Errorf("error handling event: %w", err)
	}
	return err
}

// CloseRabbitMQ cleans up RabbitMQ resources
func (es *EventStore) CloseRabbitMQ() {
	if es.Channel != nil {
		es.Channel.Close()
	}
	if es.RabbitMQ != nil {
		es.RabbitMQ.Close()
	}
}

// Broadcast sends all stored events to RabbitMQ
func (eventstore *EventStore) BroadcastWithRabbitMq() {
	eventstore.Mutex.Lock()
	defer eventstore.Mutex.Unlock()

	for {
		// Fetch an event from the pool
		event := eventstore.Events.Get().(*Event)
		if event == nil {
			break
		}

		// Publish the event to RabbitMQ
		if err := eventstore.PublishToRabbitMQ(event); err != nil {
			log.Printf("Failed to broadcast event %v: %v", event.Id, err)
		}
	}
}
