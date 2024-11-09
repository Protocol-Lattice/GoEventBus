package GoEventBus

import (
	"fmt"
	"log"
	"sync"
)

// EventStore handles publishing and dispatching events
type EventStore struct {
	Mutex      sync.Mutex
	Dispatcher *Dispatcher
	Events     *sync.Pool
}

// NewEventStore initializes an EventStore with a dispatcher and an event pool
func NewEventStore(dispatcher *Dispatcher) *EventStore {
	return &EventStore{
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
