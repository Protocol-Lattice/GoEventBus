package GoEventBus

import (
	"sync/atomic"
)

// Result represents the outcome of an event handler.
type Result struct {
	Message string
}

const size = 1 << 16

// Dispatcher maps event IDs to handler functions.
type Dispatcher map[any]func(map[string]any) (Result, error)

type Event struct {
	ID         string
	Projection any
	Args       map[string]any
}

type EventStore struct {
	dispatcher *Dispatcher // drop the pointer indirection
	buf        [size]Event
	head       uint64 // atomic write index
	tail       uint64 // next-to-read index (only by publisher)
}

func NewEventStore(dispatcher *Dispatcher) *EventStore {
	return &EventStore{dispatcher: dispatcher}
}

func (es *EventStore) Subscribe(e Event) {
	idx := atomic.AddUint64(&es.head, 1) - 1
	es.buf[idx&(size-1)] = e
}

func (es *EventStore) Publish() {
	head := atomic.LoadUint64(&es.head)
	tail := es.tail
	if tail == head {
		return
	}

	d := es.dispatcher // single indirection
	buf := es.buf[:]
	mask := uint64(size - 1)

	// only walk [tail, head)
	for i := tail; i < head; i++ {
		ev := buf[i&mask]
		if handler, ok := (*d)[ev.Projection]; ok {
			handler(ev.Args)
		}
	}

	es.tail = head
}
