package GoEventBus

import (
	"fmt"
	"sync"
)

type EventStore struct {
	Mutex      *sync.Mutex
	Dispatcher *Dispatcher
	Events     *sync.Pool
}

func NewEventStore(dispatcher *Dispatcher) *EventStore {
	return &EventStore{
		Mutex:      &sync.Mutex{},
		Dispatcher: dispatcher,
		Events: &sync.Pool{
			New: func() interface{} {
				return nil
			},
		},
	}
}

func (eventstore *EventStore) Publish(event Event) {
	eventstore.Events.Put(event)
}

func (eventstore *EventStore) Commit() error {
	curr := eventstore.Events.Get()
	if curr == nil {
		return fmt.Errorf("waiting for new events...")
	}
	event := curr.(Event)
	(*eventstore.Dispatcher)[event.Projection](event.Args)
	return nil
}

func (eventstore *EventStore) Broadcast() error {
	eventstore.Mutex.Lock()
	defer eventstore.Mutex.Unlock()
	for {
		eventstore.Commit()

	}
}
