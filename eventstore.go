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
	// DropOldest discards the oldest events when the buffer is full.
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

// HandlerFunc is the signature for event handlers and middleware.
type HandlerFunc func(context.Context, map[string]any) (Result, error)

// Middleware wraps a HandlerFunc, returning a new HandlerFunc.
type Middleware func(HandlerFunc) HandlerFunc

// Hook types for before, after, and error events.
type BeforeHook func(context.Context, Event)
type AfterHook func(context.Context, Event, Result, error)
type ErrorHook func(context.Context, Event, error)

// Dispatcher maps event projections to handler functions.
type Dispatcher map[interface{}]HandlerFunc

// Event is a unit of work to be dispatched.
type Event struct {
	ID         string
	Projection interface{}
	Args       map[string]any
}

// EventStore is a high-performance, lock-free ring buffer with middleware and hooks support.
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

	// Middleware chain and hooks
	middlewares []Middleware
	beforeHooks []BeforeHook
	afterHooks  []AfterHook
	errorHooks  []ErrorHook

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

// Use adds middleware to the EventStore. It will be applied in the order added.
func (es *EventStore) Use(mw Middleware) {
	es.middlewares = append(es.middlewares, mw)
}

// OnBefore registers a hook that runs before each handler invocation.
func (es *EventStore) OnBefore(hook BeforeHook) {
	es.beforeHooks = append(es.beforeHooks, hook)
}

// OnAfter registers a hook that runs after each handler invocation (even on error).
func (es *EventStore) OnAfter(hook AfterHook) {
	es.afterHooks = append(es.afterHooks, hook)
}

// OnError registers a hook that runs only when a handler returns an error.
func (es *EventStore) OnError(hook ErrorHook) {
	es.errorHooks = append(es.errorHooks, hook)
}

// Subscribe enqueues an Event, applying back-pressure according to OverrunPolicy.
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
			atomic.AddUint64(&es.tail, 1)
			continue
		case ReturnError:
			return ErrBufferFull
		case Block:
			runtime.Gosched()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(10 * time.Microsecond):
			}
		}
	}
}

// Publish processes all pending events, applying middleware and hooks.
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
			continue
		}
		ev := *(*Event)(p)
		if handler, ok := disp[ev.Projection]; ok {
			if es.Async {
				// fire-and-forget
				go es.execute(handler, ev)
			} else {
				es.execute(handler, ev)
			}
		}
	}
	atomic.StoreUint64(&es.tail, head)
}

// execute runs the handler with middleware and hooks.
func (es *EventStore) execute(h HandlerFunc, ev Event) {
	// establish context
	ctx := context.Background()
	if c, ok := ev.Args["__ctx"].(context.Context); ok && c != nil {
		ctx = c
	}

	// Before hooks
	for _, hook := range es.beforeHooks {
		hook(ctx, ev)
	}

	// apply middleware chain
	wrapped := h
	for i := len(es.middlewares) - 1; i >= 0; i-- {
		wrapped = es.middlewares[i](wrapped)
	}

	// invoke handler
	res, err := wrapped(ctx, ev.Args)
	atomic.AddUint64(&es.processedCount, 1)

	// After hooks
	for _, hook := range es.afterHooks {
		hook(ctx, ev, res, err)
	}

	// Error hooks and count
	if err != nil {
		atomic.AddUint64(&es.errorCount, 1)
		for _, hook := range es.errorHooks {
			hook(ctx, ev, err)
		}
	}
}

// Metrics returns snapshot counters.
func (es *EventStore) Metrics() (published, processed, errors uint64) {
	return atomic.LoadUint64(&es.publishedCount),
		atomic.LoadUint64(&es.processedCount),
		atomic.LoadUint64(&es.errorCount)
}
