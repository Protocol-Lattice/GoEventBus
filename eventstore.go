package GoEventBus

import (
	"context"
	"errors"
	"runtime"
	"sync/atomic"
	"time"
	"unsafe"
)

// Result represents the outcome of an event handler.
type Result struct {
	Message string
}

// OverrunPolicy defines what happens when the ring buffer is full.
type OverrunPolicy int

const (
	// DropOldest discards the oldest events when the buffer is full (default—behaviour prior to 2025‑04 update).
	DropOldest OverrunPolicy = iota
	// Block causes Subscribe to block (respecting ctx) until space is available.
	Block
	// ReturnError makes Subscribe fail fast with ErrBufferFull.
	ReturnError
)

// ErrBufferFull is returned by Subscribe when OverrunPolicy==ReturnError and the ring buffer is saturated.
var ErrBufferFull = errors.New("goeventbus: buffer is full")

const cacheLine = 64

type pad [cacheLine - unsafe.Sizeof(uint64(0))]byte

// Dispatcher maps event projections to handler functions that now accept context.
type Dispatcher map[interface{}]func(context.Context, map[string]any) (Result, error)

// Event is a unit of work to be dispatched.
type Event struct {
	ID         string
	Projection interface{}
	Args       map[string]any
}

// EventStore is a high‑performance, lock‑free ring buffer with optional back‑pressure.
type EventStore struct {
	dispatcher *Dispatcher
	size       uint64
	buf        []unsafe.Pointer // holds *Event
	events     []Event
	_          pad
	head       uint64 // write index
	_          pad
	tail       uint64 // read index

	// Config flags
	Async         bool
	OverrunPolicy OverrunPolicy

	// Counters
	publishedCount uint64
	processedCount uint64
	errorCount     uint64
}

// NewEventStore initializes a new EventStore.
func NewEventStore(dispatcher *Dispatcher, bufferSize uint64, policy OverrunPolicy) *EventStore {
	if bufferSize&(bufferSize-1) != 0 {
		panic("bufferSize must be a power of two")
	}
	return &EventStore{
		dispatcher:    dispatcher,
		size:          bufferSize,
		buf:           make([]unsafe.Pointer, bufferSize),
		events:        make([]Event, bufferSize),
		OverrunPolicy: policy,
	}
}

// Subscribe enqueues an Event, applying back‑pressure according to OverrunPolicy.
func (es *EventStore) Subscribe(ctx context.Context, e Event) error {
	for {
		head := atomic.LoadUint64(&es.head)
		tail := atomic.LoadUint64(&es.tail)
		if head-tail < es.size {
			// we have space
			idx := atomic.AddUint64(&es.head, 1) - 1
			slot := idx & (es.size - 1)
			ev := &es.events[slot]
			*ev = e
			atomic.StorePointer(&es.buf[slot], unsafe.Pointer(ev))
			atomic.AddUint64(&es.publishedCount, 1)
			return nil
		}

		// buffer full – resolve based on policy
		switch es.OverrunPolicy {
		case DropOldest:
			// advance tail to make room and drop oldest event
			atomic.AddUint64(&es.tail, 1)
			continue
		case ReturnError:
			return ErrBufferFull
		case Block:
			// simple back‑off sleep; yield first
			runtime.Gosched()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(10 * time.Microsecond):
			}
		}
	}
}

// Publish processes all pending events.
func (es *EventStore) Publish() {
	head := atomic.LoadUint64(&es.head)
	tail := atomic.LoadUint64(&es.tail)
	if tail == head {
		return // no new events
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
				// fire‑and‑forget goroutine
				go es.invoke(h, ev)
			} else {
				es.invoke(h, ev)
			}
		}
	}
	atomic.StoreUint64(&es.tail, head)
}

func (es *EventStore) invoke(h func(context.Context, map[string]any) (Result, error), ev Event) {
	// invoke with background if no context provided (legacy support)
	ctx := context.Background()
	if c, ok := ev.Args["__ctx"].(context.Context); ok && c != nil {
		ctx = c
	}
	_, err := h(ctx, ev.Args)
	atomic.AddUint64(&es.processedCount, 1)
	if err != nil {
		atomic.AddUint64(&es.errorCount, 1)
	}
}

// Metrics returns snapshot counters.
func (es *EventStore) Metrics() (published, processed, errors uint64) {
	return atomic.LoadUint64(&es.publishedCount),
		atomic.LoadUint64(&es.processedCount),
		atomic.LoadUint64(&es.errorCount)
}
