package GoEventBus

import (
	"context"
	"errors"
	"runtime"
	"sync"
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
	Ctx        context.Context // carried context from Subscribe
}

// internal work unit for async dispatch
type eventWork struct {
	handler HandlerFunc
	ev      Event
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

	// Async worker pool
	asyncWorkers   int
	workCh         chan eventWork
	wg             sync.WaitGroup
	shutdownOnce   sync.Once
	shutdownSignal chan struct{}

	// Counters
	publishedCount uint64
	processedCount uint64
	errorCount     uint64
}

// NewEventStore initializes a new EventStore. It spins up a default worker pool.
func NewEventStore(dispatcher *Dispatcher, bufferSize uint64, policy OverrunPolicy) *EventStore {
	if bufferSize&(bufferSize-1) != 0 {
		panic("bufferSize must be a power of two")
	}
	es := &EventStore{
		dispatcher:     dispatcher,
		size:           bufferSize,
		buf:            make([]unsafe.Pointer, bufferSize),
		events:         make([]Event, bufferSize),
		OverrunPolicy:  policy,
		asyncWorkers:   runtime.NumCPU(),
		workCh:         make(chan eventWork, bufferSize),
		shutdownSignal: make(chan struct{}),
	}
	// start worker pool
	for i := 0; i < es.asyncWorkers; i++ {
		go es.worker()
	}
	return es
}

// worker processes eventWork from the channel until shutdown.
func (es *EventStore) worker() {
	for w := range es.workCh {
		es.execute(w.handler, w.ev)
		es.wg.Done()
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
	// record caller context
	e.Ctx = ctx
	for {
		head := atomic.LoadUint64(&es.head)
		tail := atomic.LoadUint64(&es.tail)
		if head-tail < es.size {
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
		return
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
				es.wg.Add(1)
				select {
				case es.workCh <- eventWork{handler, ev}:
				case <-es.shutdownSignal:
					es.wg.Done()
				}
			} else {
				es.execute(handler, ev)
			}
		}
	}
	atomic.StoreUint64(&es.tail, head)
}

// execute runs the handler with middleware and hooks.
func (es *EventStore) execute(h HandlerFunc, ev Event) {
	// pick up recorded context
	ctx := ev.Ctx
	if ctx == nil {
		ctx = context.Background()
	}
	// override with explicit __ctx if set
	if c, ok := ev.Args["__ctx"].(context.Context); ok && c != nil {
		ctx = c
	}

	for _, hook := range es.beforeHooks {
		hook(ctx, ev)
	}
	wrapped := h
	for i := len(es.middlewares) - 1; i >= 0; i-- {
		wrapped = es.middlewares[i](wrapped)
	}
	res, err := wrapped(ctx, ev.Args)
	atomic.AddUint64(&es.processedCount, 1)

	for _, hook := range es.afterHooks {
		hook(ctx, ev, res, err)
	}
	if err != nil {
		atomic.AddUint64(&es.errorCount, 1)
		for _, hook := range es.errorHooks {
			hook(ctx, ev, err)
		}
	}
}

// Drain waits for all in-flight async handlers to complete, stopping new dispatch.
func (es *EventStore) Drain(ctx context.Context) error {
	es.shutdownOnce.Do(func() {
		close(es.shutdownSignal)
		close(es.workCh)
	})
	done := make(chan struct{})
	go func() {
		es.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Metrics returns snapshot counters.
func (es *EventStore) Metrics() (published, processed, errors uint64) {
	return atomic.LoadUint64(&es.publishedCount),
		atomic.LoadUint64(&es.processedCount),
		atomic.LoadUint64(&es.errorCount)
}
