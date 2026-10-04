package GoEventBus

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
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

var (
	// ErrBufferFull is returned by Subscribe when OverrunPolicy==ReturnError and the ring buffer is saturated.
	ErrBufferFull = errors.New("goeventbus: buffer is full")
	// ErrEventStoreClosed is returned by Subscribe after Drain or Close has
	// started. Events accepted before that point are dispatched before Drain or
	// Close returns successfully.
	ErrEventStoreClosed = errors.New("goeventbus: event store is closed")
)

const cacheLine = 64

type pad [cacheLine - 8]byte

// ringSlot is a cell in the bounded MPMC ring. sequence owns publication of
// event: a producer writes event before advancing sequence, and a consumer
// reads event before releasing the slot for its next producer.
type ringSlot struct {
	sequence atomic.Uint64
	event    Event
}

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

// EventStore is a bounded MPMC ring buffer with middleware and hooks support.
//
// Subscribe and Publish are safe to call concurrently. Configuration methods
// (Use, OnBefore, OnAfter, OnError, RegisterBatch, and RegisterOrdered) must
// be called before concurrent use begins.
type EventStore struct {
	dispatcher *Dispatcher
	size       uint64
	slots      []ringSlot
	_          pad
	enqueuePos atomic.Uint64
	_          pad
	dequeuePos atomic.Uint64

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
	asyncWorkers int
	workCh       chan work
	wg           sync.WaitGroup
	// orderedDispatchMu establishes the enqueue order for ordered handlers
	// across concurrent Publish calls.
	orderedDispatchMu sync.Mutex
	lifecycleMu       sync.Mutex
	activeOps         sync.WaitGroup
	shutdownOnce      sync.Once
	shutdownDone      chan struct{}
	closed            atomic.Bool
	shutdownErr       error

	// Optional external provider configured through NewEventStore options.
	providerMu      sync.Mutex
	provider        Provider
	providerFactory func() (Provider, error)

	// Counters
	publishedCount uint64
	processedCount uint64
	errorCount     uint64

	// DLQ, when non-nil, receives every event that fails or panics during dispatch.
	DLQ *DeadLetterQueue
}

// NewEventStore initializes a new EventStore. It spins up a default worker pool.
//
// Optional integrations such as Redis Streams and RabbitMQ can be attached with
// EventStoreOption values while the original three-argument API remains valid.
func NewEventStore(dispatcher *Dispatcher, bufferSize uint64, policy OverrunPolicy, options ...EventStoreOption) *EventStore {
	if dispatcher == nil {
		panic("GoEventBus: dispatcher must not be nil")
	}
	if bufferSize == 0 || bufferSize&(bufferSize-1) != 0 {
		panic("GoEventBus: bufferSize must be a non-zero power of two")
	}
	es := &EventStore{
		dispatcher:      dispatcher,
		size:            bufferSize,
		slots:           make([]ringSlot, bufferSize),
		OverrunPolicy:   policy,
		batchHandlers:   make(map[interface{}][]batchEntry),
		orderedHandlers: make(map[interface{}][]*orderedHandler),
		asyncWorkers:    runtime.NumCPU(),
		workCh:          make(chan work, bufferSize),
		shutdownDone:    make(chan struct{}),
	}
	for i := range es.slots {
		es.slots[i].sequence.Store(uint64(i) * 2)
	}
	for _, option := range options {
		if option == nil {
			panic("GoEventBus: EventStore option must not be nil")
		}
		if err := option(es); err != nil {
			if es.provider != nil {
				_ = es.provider.Close()
			}
			panic(fmt.Sprintf("GoEventBus: configure EventStore: %v", err))
		}
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
// Ordered enqueue is serialized across concurrent Publish calls, so a key
// retains ring dequeue order in asynchronous mode. In synchronous mode,
// handlers run in the caller goroutine; callers that need invocation ordering
// must serialize their Publish calls. keyFn must not call Publish, Drain, or
// Close on this store.
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

// enterOperation prevents shutdown from closing the worker channel while a
// producer or dispatcher can still use it.
func (es *EventStore) enterOperation() bool {
	es.lifecycleMu.Lock()
	defer es.lifecycleMu.Unlock()
	if es.closed.Load() {
		return false
	}
	es.activeOps.Add(1)
	return true
}

// tryEnqueue inserts ev using per-slot sequence numbers. It is the bounded
// MPMC algorithm: claiming a position does not make its event visible until
// its slot sequence is advanced after the event write.
func (es *EventStore) tryEnqueue(ev Event) bool {
	pos := es.enqueuePos.Load()
	for {
		slot := &es.slots[pos&(es.size-1)]
		sequence := slot.sequence.Load()
		expected := pos * 2
		diff := int64(sequence) - int64(expected)
		switch {
		case diff == 0:
			if es.enqueuePos.CompareAndSwap(pos, pos+1) {
				slot.event = ev
				slot.sequence.Store(expected + 1)
				return true
			}
		case diff < 0:
			return false
		}
		pos = es.enqueuePos.Load()
	}
}

// tryDequeue removes one event. A false result means the next FIFO position
// has not yet been published or the ring is empty; a later Publish can retry.
func (es *EventStore) tryDequeue() (Event, bool) {
	pos := es.dequeuePos.Load()
	for {
		slot := &es.slots[pos&(es.size-1)]
		sequence := slot.sequence.Load()
		expected := pos*2 + 1
		diff := int64(sequence) - int64(expected)
		switch {
		case diff == 0:
			if es.dequeuePos.CompareAndSwap(pos, pos+1) {
				ev := slot.event
				slot.sequence.Store((pos + es.size) * 2)
				return ev, true
			}
		case diff < 0:
			return Event{}, false
		}
		pos = es.dequeuePos.Load()
	}
}

// Subscribe enqueues an Event, applying back-pressure according to
// OverrunPolicy. It returns ErrEventStoreClosed when Drain or Close has begun.
func (es *EventStore) Subscribe(ctx context.Context, e Event) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if !es.enterOperation() {
		return ErrEventStoreClosed
	}
	defer es.activeOps.Done()

	e.Ctx = ctx
	for {
		if es.closed.Load() {
			return ErrEventStoreClosed
		}
		if es.tryEnqueue(e) {
			atomic.AddUint64(&es.publishedCount, 1)
			return nil
		}

		switch es.OverrunPolicy {
		case DropOldest:
			// Eviction is a regular dequeue claim, so it cannot race with a
			// concurrent Publish and accidentally discard a new event.
			if _, discarded := es.tryDequeue(); discarded {
				continue
			}
			runtime.Gosched()
		case ReturnError:
			return ErrBufferFull
		case Block:
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			timer := time.NewTimer(10 * time.Microsecond)
			select {
			case <-ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				return ctx.Err()
			case <-timer.C:
			}
		}
	}
}

// takePending claims the currently available FIFO prefix. It intentionally
// avoids allocating until it has actually claimed an event, keeping empty
// Publish calls allocation-free even for large rings.
func (es *EventStore) takePending() []Event {
	ev, ok := es.tryDequeue()
	if !ok {
		return nil
	}
	events := make([]Event, 0, 16)
	events = append(events, ev)
	for {
		ev, ok = es.tryDequeue()
		if !ok {
			break
		}
		events = append(events, ev)
	}
	return events
}

// Publish processes all pending events, applying middleware and hooks.
// Regular handlers receive one event at a time. Batch handlers registered via
// RegisterBatch receive events grouped by projection in chunks of up to their
// configured size.
func (es *EventStore) Publish() {
	if !es.enterOperation() {
		return
	}
	defer es.activeOps.Done()

	// An ordered handler's queue must be populated in dequeue order. Claiming
	// and enqueueing ordered work under one lock prevents a later Publish from
	// overtaking an earlier event between those two steps.
	if es.Async && len(es.orderedHandlers) > 0 {
		events, orderedWork := es.takeAndQueueOrdered()
		es.submitOrdered(orderedWork)
		es.dispatch(events, false)
		return
	}
	es.dispatch(es.takePending(), true)
}

func (es *EventStore) dispatch(events []Event, dispatchOrdered bool) {
	if len(events) == 0 {
		return
	}

	disp := *es.dispatcher
	hasBatch := len(es.batchHandlers) > 0

	var batchGroups map[interface{}][]Event
	if hasBatch {
		batchGroups = make(map[interface{}][]Event)
	}

	for _, ev := range events {
		if handlers, ok := disp[ev.Projection]; ok {
			for _, handler := range handlers {
				if es.Async {
					es.wg.Add(1)
					es.workCh <- work{kind: workSingle, handler: handler, ev: ev}
				} else {
					es.execute(handler, ev)
				}
			}
		}
		if dispatchOrdered {
			if handlers, ok := es.orderedHandlers[ev.Projection]; ok {
				for _, handler := range handlers {
					if es.Async {
						es.enqueueOrdered(handler, ev)
					} else {
						es.execute(handler.handler, ev)
					}
				}
			}
		}
		if hasBatch {
			if _, ok := es.batchHandlers[ev.Projection]; ok {
				batchGroups[ev.Projection] = append(batchGroups[ev.Projection], ev)
			}
		}
	}

	for proj, entries := range es.batchHandlers {
		events := batchGroups[proj]
		for _, entry := range entries {
			for start := 0; start < len(events); start += entry.size {
				end := start + entry.size
				if end > len(events) {
					end = len(events)
				}
				chunk := events[start:end]
				if es.Async {
					es.wg.Add(1)
					es.workCh <- work{kind: workBatch, batchFn: entry.handler, events: chunk}
				} else {
					es.executeBatch(entry.handler, chunk)
				}
			}
		}
	}
}

func (es *EventStore) queueOrderedEvents(events []Event) []work {
	var workItems []work
	for _, ev := range events {
		for _, handler := range es.orderedHandlers[ev.Projection] {
			if item, shouldStart := es.queueOrdered(handler, ev); shouldStart {
				workItems = append(workItems, item)
			}
		}
	}
	return workItems
}

func (es *EventStore) takeAndQueueOrdered() ([]Event, []work) {
	es.orderedDispatchMu.Lock()
	defer es.orderedDispatchMu.Unlock()
	events := es.takePending()
	return events, es.queueOrderedEvents(events)
}

// queueOrdered appends ev to its handler/key queue. The caller schedules the
// returned work only after it has released any ordering lock.
func (es *EventStore) queueOrdered(handler *orderedHandler, ev Event) (work, bool) {
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
		return work{}, false
	}
	queue.running = true
	handler.mu.Unlock()

	return work{kind: workOrdered, orderedHandler: handler, orderingKey: key}, true
}

// enqueueOrdered appends ev to its handler/key queue and schedules the first
// worker for that key. It is used only by the non-serialized fallback path.
func (es *EventStore) enqueueOrdered(handler *orderedHandler, ev Event) {
	if item, shouldStart := es.queueOrdered(handler, ev); shouldStart {
		es.submitOrdered([]work{item})
	}
}

func (es *EventStore) submitOrdered(workItems []work) {
	for _, item := range workItems {
		es.wg.Add(1)
		es.workCh <- item
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
func (es *EventStore) execute(h HandlerFunc, ev Event) (returnedErr error) {
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
			returnedErr = panicErr
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
	return err
}

// beginShutdown makes the transition from accepting work to closed exactly
// once. It waits for operations already in progress, dispatches their pending
// events, then closes workers only after no sender can remain.
func (es *EventStore) beginShutdown() {
	es.lifecycleMu.Lock()
	es.closed.Store(true)
	es.lifecycleMu.Unlock()

	es.activeOps.Wait()
	es.shutdownErr = es.closeConfiguredProvider()
	if es.Async && len(es.orderedHandlers) > 0 {
		events, orderedWork := es.takeAndQueueOrdered()
		es.submitOrdered(orderedWork)
		es.dispatch(events, false)
	} else {
		es.dispatch(es.takePending(), true)
	}
	close(es.workCh)
	es.wg.Wait()
	close(es.shutdownDone)
}

// Drain stops accepting new events, dispatches events already accepted, and
// waits for all asynchronous handlers to complete. A timed-out Drain leaves
// shutdown in progress; a later Drain or Close can wait for the same result.
// It must be called outside an EventStore handler, because waiting for the
// current handler from that handler would deadlock.
func (es *EventStore) Drain(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	es.shutdownOnce.Do(func() {
		go es.beginShutdown()
	})
	select {
	case <-es.shutdownDone:
		return es.shutdownErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close is an alias for Drain.
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
