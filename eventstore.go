package GoEventBus

import (
	"sync/atomic"
	"unsafe"
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

const cacheLine = 64

type pad [cacheLine - unsafe.Sizeof(uint64(0))]byte

// EventStore is a high-performance, lock-free ring buffer.
type EventStore struct {
	dispatcher *Dispatcher // handler registry (read-only)
	buf        [size]Event // circular buffer

	_     pad    // pad out to a full cache line
	head  uint64 // atomic write index
	_     pad    // pad again
	tail  uint64 // atomic read index
	Async bool   // <-- Add this flag
}

func NewEventStore(dispatcher *Dispatcher) *EventStore {
	return &EventStore{dispatcher: dispatcher}
}

// Subscribe just claims a slot and writes—no Load/Store on tail here.
func (es *EventStore) Subscribe(e Event) {
	idx := atomic.AddUint64(&es.head, 1) - 1
	es.buf[idx&(size-1)] = e
}

// Publish now also drops old events if we’ve overrun the ring.
func (es *EventStore) Publish() {
	head := atomic.LoadUint64(&es.head)
	tail := atomic.LoadUint64(&es.tail)
	if tail == head {
		return
	}

	if head-tail > size {
		tail = head - size
	}

	disp := *es.dispatcher
	buf := es.buf[:]
	mask := uint64(size - 1)

	for i := tail; i < head; i++ {
		ev := buf[i&mask]
		if h, ok := disp[ev.Projection]; ok {
			if es.Async {
				go h(ev.Args) // async handler call
			} else {
				h(ev.Args) // sync handler call
			}
		}
	}
	atomic.StoreUint64(&es.tail, head)
}
