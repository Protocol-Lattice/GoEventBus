package GoEventBus

import (
	"sync/atomic"
)

// Result represents the outcome of an event handler.
type Result struct {
	Message string
}

const size = 1 << 16

// Dispatcher maps event projections to handler functions.
type Dispatcher map[interface{}]func(map[string]any) (Result, error)

type Event struct {
	ID         string
	Projection interface{}
	Args       map[string]any
}

// EventStore is a high-performance, lock-free ring buffer.
type EventStore struct {
	dispatcher *Dispatcher // handler registry (read-only)
	buf        [size]Event // circular buffer
	head       uint64      // atomic write index
	tail       uint64      // atomic read index
}

// NewEventStore initializes a new EventStore with handlers.
func NewEventStore(dispatcher *Dispatcher) *EventStore {
	return &EventStore{dispatcher: dispatcher}
}

// Subscribe enqueues an event; if full, drops the oldest.
func (es *EventStore) Subscribe(e Event) {
	idx := atomic.AddUint64(&es.head, 1) - 1

	// drop oldest if buffer overflows
	t := atomic.LoadUint64(&es.tail)
	if idx-t >= size {
		atomic.StoreUint64(&es.tail, idx-size+1)
	}

	es.buf[idx&(size-1)] = e
}

// Publish invokes handlers for all pending events.
func (es *EventStore) Publish() {
	head := atomic.LoadUint64(&es.head)
	tail := atomic.LoadUint64(&es.tail)
	if tail == head {
		return
	}

	// cache locals for speed
	disp := *es.dispatcher
	buf := es.buf[:]
	mask := uint64(size - 1)

	for i := tail; i < head; i++ {
		ev := buf[i&mask]
		if h, ok := disp[ev.Projection]; ok {
			h(ev.Args)
		}
	}

	atomic.StoreUint64(&es.tail, head)
}
