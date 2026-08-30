package GoEventBus

import (
	"context"
	"errors"
	"fmt"
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
type HandlerFunc func(context.Context, Event) (Result, error)

// Middleware wraps a HandlerFunc, returning a new HandlerFunc.
type Middleware func(HandlerFunc) HandlerFunc

// OrderingKeyFunc returns the partition key for an ordered handler. Events
// with the same key are delivered to that handler in publish order when the
// store is running asynchronously.
type OrderingKeyFunc func(Event) string

// Hook types for before, after, and error events.
type BeforeHook func(context.Context, Event)
type AfterHook func(context.Context, Event, Result, error)
type ErrorHook func(context.Context, Event, error)

// Dispatcher maps event projections to one or more handler functions.
// Multiple handlers registered for the same projection are called in order (fan-out).
type Dispatcher map[interface{}][]HandlerFunc

// Register appends one or more handlers for the given projection.
// Calling Register multiple times on the same key accumulates handlers.
func (d Dispatcher) Register(projection interface{}, handlers ...HandlerFunc) {
	d[projection] = append(d[projection], handlers...)
}

// Event is a unit of work to be dispatched.
type Event struct {
	ID         string
	Projection interface{}
	Data       any             // Type-safe payload (preferred)
	Args       map[string]any  // Legacy payload (deprecated)
	Ctx        context.Context // carried context from Subscribe
}

// internal work unit for async dispatch
type workKind uint8

const (
	workSingle workKind = iota
	workBatch
	workOrdered
)

type work struct {
	kind           workKind
	handler        HandlerFunc      // workSingle
	ev             Event            // workSingle
	batchFn        BatchHandlerFunc // workBatch
	events         []Event          // workBatch
	orderedHandler *orderedHandler  // workOrdered
	orderingKey    string           // workOrdered
}

// orderedHandler owns a serial queue for each ordering key. Queues are local
// to a handler so separate ordered handlers remain independent fan-out
// consumers.
type orderedHandler struct {
	handler HandlerFunc
	keyFn   OrderingKeyFunc

	mu     sync.Mutex
	queues map[string]*orderedQueue
}

type orderedQueue struct {
	events  []Event
	running bool
}

// EventStore is a high-performance, lock-free ring buffer with middleware and hooks support.
type EventStore struct {
	dispatcher *Dispatcher
	size       uint64
	buf        []atomic.Pointer[Event] // holds *Event pointers safely
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

	// Batch handlers (projection → []batchEntry); populated via RegisterBatch.
	batchHandlers map[interface{}][]batchEntry

	// Ordered handlers (projection → handlers); populated via RegisterOrdered.
	orderedHandlers map[interface{}][]*orderedHandler

	// Async worker pool
	asyncWorkers   int
	workCh         chan work
	wg             sync.WaitGroup
	shutdownOnce   sync.Once
	shutdownSignal chan struct{}

	// Counters
	publishedCount uint64
	processedCount uint64
	errorCount     uint64

	// txMu serialises Rollback against concurrent Subscribe calls.
	txMu sync.Mutex

	// DLQ, when non-nil, receives every event that fails or panics during dispatch.
	DLQ *DeadLetterQueue
}

// NewEventStore initializes a new EventStore. It spins up a default worker pool.
func NewEventStore(dispatcher *Dispatcher, bufferSize uint64, policy OverrunPolicy) *EventStore {
	if dispatcher == nil {
		panic("GoEventBus: dispatcher must not be nil")
	}
	if bufferSize == 0 || bufferSize&(bufferSize-1) != 0 {
		panic("GoEventBus: bufferSize must be a non-zero power of two")
	}
	es := &EventStore{
		dispatcher:      dispatcher,
		size:            bufferSize,
		buf:             make([]atomic.Pointer[Event], bufferSize),
		events:          make([]Event, bufferSize),
		OverrunPolicy:   policy,
		batchHandlers:   make(map[interface{}][]batchEntry),
		orderedHandlers: make(map[interface{}][]*orderedHandler),
		asyncWorkers:    runtime.NumCPU(),
		workCh:          make(chan work, bufferSize),
		shutdownSignal:  make(chan struct{}),
	}
	// start worker pool
	for i := 0; i < es.asyncWorkers; i++ {
		go es.worker()
	}
	return es
}

// worker processes work items from the channel until shutdown.
func (es *EventStore) worker() {
	for w := range es.workCh {
		func(w work) {
			defer es.wg.Done()
			switch w.kind {
			case workSingle:
				es.execute(w.handler, w.ev)
			case workBatch:
				es.executeBatch(w.batchFn, w.events)
			case workOrdered:
				es.executeOrdered(w.orderedHandler, w.orderingKey)
			}
		}(w)
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

// RegisterOrdered registers handlers whose events are processed in order for
// each key returned by keyFn. In asynchronous mode, events with different keys
// can still be processed concurrently. Multiple ordered handlers registered
// for the same projection are independent fan-out consumers.
//
// In synchronous mode all handlers are already invoked serially, so keyFn is
// not called and handlers run as regular synchronous handlers.
func (es *EventStore) RegisterOrdered(projection interface{}, keyFn OrderingKeyFunc, handlers ...HandlerFunc) {
	if keyFn == nil {
		panic("GoEventBus: ordering key function must not be nil")
	}
	for _, handler := range handlers {
		if handler == nil {
			panic("GoEventBus: ordered handler must not be nil")
		}
		es.orderedHandlers[projection] = append(es.orderedHandlers[projection], &orderedHandler{
			handler: handler,
			keyFn:   keyFn,
			queues:  make(map[string]*orderedQueue),
		})
	}
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
			evPtr := &es.events[slot]
			*evPtr = e
			es.buf[slot].Store(evPtr)
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
// Regular handlers receive one event at a time. Batch handlers registered via
// RegisterBatch receive events grouped by projection in chunks of up to their
// configured size.
func (es *EventStore) Publish() {
	head := atomic.LoadUint64(&es.head)
	tail := atomic.LoadUint64(&es.tail)
	if tail == head {
		return
	}

	disp := *es.dispatcher
	mask := es.size - 1
	hasBatch := len(es.batchHandlers) > 0

	// batchGroups collects events per projection for batch dispatch.
	var batchGroups map[interface{}][]Event
	if hasBatch {
		batchGroups = make(map[interface{}][]Event)
	}

	for i := tail; i < head; i++ {
		p := es.buf[i&mask].Load()
		if p == nil {
			continue
		}
		ev := *p
		if handlers, ok := disp[ev.Projection]; ok {
			for _, handler := range handlers {
				if es.Async {
					es.wg.Add(1)
					select {
					case es.workCh <- work{kind: workSingle, handler: handler, ev: ev}:
					case <-es.shutdownSignal:
						es.wg.Done()
					}
				} else {
					es.execute(handler, ev)
				}
			}
		}
		if handlers, ok := es.orderedHandlers[ev.Projection]; ok {
			for _, handler := range handlers {
				if es.Async {
					es.enqueueOrdered(handler, ev)
				} else {
					es.execute(handler.handler, ev)
				}
			}
		}
		if hasBatch {
			if _, ok := es.batchHandlers[ev.Projection]; ok {
				batchGroups[ev.Projection] = append(batchGroups[ev.Projection], ev)
			}
		}
	}

	// Dispatch batch handlers in chunks.
	for proj, entries := range es.batchHandlers {
		events := batchGroups[proj]
		if len(events) == 0 {
			continue
		}
		for _, entry := range entries {
			for start := 0; start < len(events); start += entry.size {
				end := start + entry.size
				if end > len(events) {
					end = len(events)
				}
				chunk := events[start:end]
				if es.Async {
					es.wg.Add(1)
					select {
					case es.workCh <- work{kind: workBatch, batchFn: entry.handler, events: chunk}:
					case <-es.shutdownSignal:
						es.wg.Done()
					}
				} else {
					es.executeBatch(entry.handler, chunk)
				}
			}
		}
	}

	atomic.StoreUint64(&es.tail, head)
}

// enqueueOrdered appends ev to its handler/key queue. The first event in a
// queue schedules one worker task; that task drains the queue serially. This
// keeps a key ordered without dedicating a goroutine to every key.
func (es *EventStore) enqueueOrdered(handler *orderedHandler, ev Event) {
	key := handler.keyFn(ev)

	handler.mu.Lock()
	queue := handler.queues[key]
	if queue == nil {
		queue = &orderedQueue{}
		handler.queues[key] = queue
	}
	queue.events = append(queue.events, ev)
	if queue.running {
		handler.mu.Unlock()
		return
	}
	queue.running = true
	handler.mu.Unlock()

	es.wg.Add(1)
	select {
	case es.workCh <- work{kind: workOrdered, orderedHandler: handler, orderingKey: key}:
	case <-es.shutdownSignal:
		es.wg.Done()
		// Drain has stopped dispatch. Mark the queue idle so it is not left in
		// a misleading running state if a caller inspects it during shutdown.
		handler.mu.Lock()
		queue.running = false
		handler.mu.Unlock()
	}
}

// executeOrdered drains one handler/key queue. New events added while an
// event is executing are picked up by the same worker before it releases the
// key, preserving FIFO delivery for that key.
func (es *EventStore) executeOrdered(handler *orderedHandler, key string) {
	for {
		handler.mu.Lock()
		queue := handler.queues[key]
		if queue == nil || len(queue.events) == 0 {
			delete(handler.queues, key)
			handler.mu.Unlock()
			return
		}
		ev := queue.events[0]
		queue.events[0] = Event{} // release references held by the queue
		queue.events = queue.events[1:]
		handler.mu.Unlock()

		es.execute(handler.handler, ev)
	}
}

// execute runs the handler with middleware and hooks.
// It recovers from panics, treating them as errors so the DLQ and error hooks
// still fire and the caller (sync or worker goroutine) is never killed.
func (es *EventStore) execute(h HandlerFunc, ev Event) {
	ctx := ev.Ctx
	if ctx == nil {
		ctx = context.Background()
	}

	defer func() {
		if r := recover(); r != nil {
			var panicErr error
			if e, ok := r.(error); ok {
				panicErr = fmt.Errorf("handler panic: %w", e)
			} else {
				panicErr = fmt.Errorf("handler panic: %v", r)
			}
			atomic.AddUint64(&es.errorCount, 1)
			if es.DLQ != nil {
				es.DLQ.add(DeadLetter{Event: ev, Err: panicErr, FailedAt: time.Now(), Attempts: 1})
			}
			for _, hook := range es.errorHooks {
				hook(ctx, ev, panicErr)
			}
		}
	}()

	for _, hook := range es.beforeHooks {
		hook(ctx, ev)
	}
	wrapped := h
	for i := len(es.middlewares) - 1; i >= 0; i-- {
		wrapped = es.middlewares[i](wrapped)
	}
	res, err := wrapped(ctx, ev)
	atomic.AddUint64(&es.processedCount, 1)

	for _, hook := range es.afterHooks {
		hook(ctx, ev, res, err)
	}
	if err != nil {
		atomic.AddUint64(&es.errorCount, 1)
		if es.DLQ != nil {
			es.DLQ.add(DeadLetter{Event: ev, Err: err, FailedAt: time.Now(), Attempts: 1})
		}
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

// Close drains all pending async events and shuts down the EventStore.
func (es *EventStore) Close(ctx context.Context) error {
	return es.Drain(ctx)
}

// Metrics returns snapshot counters.
func (es *EventStore) Metrics() (published, processed, errors uint64) {
	return atomic.LoadUint64(&es.publishedCount),
		atomic.LoadUint64(&es.processedCount),
		atomic.LoadUint64(&es.errorCount)
}

func (es *EventStore) Schedule(ctx context.Context, t time.Time, e Event) *time.Timer {
	delay := time.Until(t)
	if delay <= 0 {
		// immediate, synchronous execution
		e.Ctx = ctx
		// look up and run the handler directly
		disp := *es.dispatcher
		for _, handler := range disp[e.Projection] {
			es.execute(handler, e)
		}
		return nil
	}

	// schedule for the future via time.AfterFunc
	return time.AfterFunc(delay, func() {
		_ = es.Subscribe(ctx, e)
		es.Publish()
	})
}

// ScheduleAfter fires e after the given duration d.
// If d<=0 it falls back to Schedule(now).
func (es *EventStore) ScheduleAfter(ctx context.Context, d time.Duration, e Event) *time.Timer {
	if d <= 0 {
		return es.Schedule(ctx, time.Now(), e)
	}
	return time.AfterFunc(d, func() {
		_ = es.Subscribe(ctx, e)
		es.Publish()
	})
}
