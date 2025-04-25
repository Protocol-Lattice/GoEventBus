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

// Event is a unit of work to be dispatched.
type Event struct {
	ID         string
	Projection interface{}
	Args       map[string]any
}

const cacheLine = 64

type pad [cacheLine - unsafe.Sizeof(uint64(0))]byte

// EventStore is a high-performance, lock-free ring buffer using atomic pointer ops.
type EventStore struct {
	dispatcher *Dispatcher
	size       uint64
	buf        []unsafe.Pointer
	events     []Event
	_          pad
	head       uint64
	_          pad
	tail       uint64
	Async      bool

	publishedCount uint64
	processedCount uint64
	errorCount     uint64
}

// NewEventStore initializes a new EventStore with the given dispatcher.
func NewEventStore(dispatcher *Dispatcher, bufferSize uint64) *EventStore {
	// bufferSize must be a power of two
	if bufferSize&(bufferSize-1) != 0 {
		panic("bufferSize must be a power of two")
	}
	return &EventStore{
		dispatcher: dispatcher,
		size:       bufferSize,
		buf:        make([]unsafe.Pointer, bufferSize),
		events:     make([]Event, bufferSize),
	}
}

// Subscribe atomically enqueues an Event by storing its pointer.
func (es *EventStore) Subscribe(e Event) {
	atomic.AddUint64(&es.publishedCount, 1)

	idx := atomic.AddUint64(&es.head, 1) - 1
	slot := idx & (es.size - 1)
	// copy into the existing slot—no heap allocation
	ev := &es.events[slot]
	*ev = e

	// publish pointer to that slot
	atomic.StorePointer(&es.buf[slot], unsafe.Pointer(ev))
}

// Publish processes all pending events, dropping old ones on overflow.
func (es *EventStore) Publish() {
	head := atomic.LoadUint64(&es.head)
	tail := atomic.LoadUint64(&es.tail)
	if tail == head {
		return // no new events
	}
	// drop oldest if buffer overrun
	if head-tail > es.size {
		tail = head - es.size
	}

	disp := *es.dispatcher
	mask := es.size - 1

	for i := tail; i < head; i++ {
		p := atomic.LoadPointer(&es.buf[i&mask])
		if p == nil {
			continue // slot not written yet
		}
		ev := *(*Event)(p)
		if h, ok := disp[ev.Projection]; ok {
			if es.Async {
				go func(args map[string]any) {
					_, err := h(args)
					atomic.AddUint64(&es.processedCount, 1)
					if err != nil {
						atomic.AddUint64(&es.errorCount, 1)
					}
				}(ev.Args)
			} else {
				_, err := h(ev.Args)
				atomic.AddUint64(&es.processedCount, 1)
				if err != nil {
					atomic.AddUint64(&es.errorCount, 1)
				}
			}
		}
	}
	atomic.StoreUint64(&es.tail, head)
}

// Metrics returns current counters for published, processed and errored events.
func (es *EventStore) Metrics() (published, processed, errors uint64) {
	return atomic.LoadUint64(&es.publishedCount),
		atomic.LoadUint64(&es.processedCount),
		atomic.LoadUint64(&es.errorCount)
}
