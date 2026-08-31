package GoEventBus

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/valyala/fasthttp"
)

const size = 1 << 16

// helper context value
var bg = context.Background()

// hs wraps a single HandlerFunc into the []HandlerFunc slice that Dispatcher expects.
func hs(fn HandlerFunc) []HandlerFunc { return []HandlerFunc{fn} }

// TestSubscribeAndPublish verifies that events are stored and published correctly.
func TestSubscribeAndPublish(t *testing.T) {
	dispatcher := Dispatcher{}
	var called1, called2 int32
	dispatcher.Register("evt1", func(_ context.Context, ev Event) (Result, error) {
		atomic.AddInt32(&called1, 1)
		return Result{Message: "ok1"}, nil
	})
	dispatcher.Register("evt2", func(_ context.Context, ev Event) (Result, error) {
		atomic.AddInt32(&called2, 1)
		return Result{Message: "ok2"}, nil
	})

	es := NewEventStore(&dispatcher, 1<<16, DropOldest)
	_ = es.Subscribe(bg, Event{ID: "1", Projection: "evt1", Args: nil})
	_ = es.Subscribe(bg, Event{ID: "2", Projection: "evt2", Args: nil})

	es.Publish()

	if called1 != 1 {
		t.Errorf("handler evt1 called %d times; want 1", called1)
	}
	if called2 != 1 {
		t.Errorf("handler evt2 called %d times; want 1", called2)
	}
}

// TestPublishWithMissingHandler ensures no panic when a handler is missing.
func TestPublishWithMissingHandler(t *testing.T) {
	dispatcher := Dispatcher{}
	es := NewEventStore(&dispatcher, 1<<16, DropOldest)
	_ = es.Subscribe(bg, Event{ID: "3", Projection: "unknown", Args: nil})
	es.Publish() // should not panic
}

// TestPublishMixedExistingAndNonExisting ensures missing projections don't affect existing handlers.
func TestPublishMixedExistingAndNonExisting(t *testing.T) {
	dispatcher := Dispatcher{}
	var called int32
	dispatcher.Register("evt", func(_ context.Context, ev Event) (Result, error) {
		atomic.AddInt32(&called, 1)
		return Result{}, nil
	})
	es := NewEventStore(&dispatcher, 1<<16, DropOldest)
	_ = es.Subscribe(bg, Event{ID: "1", Projection: "evt", Args: nil})
	_ = es.Subscribe(bg, Event{ID: "2", Projection: "noexist", Args: nil})
	es.Publish()

	if called != 1 {
		t.Errorf("handler called %d times; want 1", called)
	}
}

// TestOverflowBehavior ensures that when more events than buffer size are enqueued, the oldest events are dropped.
func TestOverflowBehavior(t *testing.T) {
	dispatcher := Dispatcher{}
	var count uint64
	dispatcher.Register("evtOverflow", func(_ context.Context, ev Event) (Result, error) {
		atomic.AddUint64(&count, 1)
		return Result{}, nil
	})
	es := NewEventStore(&dispatcher, 1<<16, DropOldest)
	for i := 0; i < size+100; i++ {
		_ = es.Subscribe(bg, Event{ID: "o", Projection: "evtOverflow", Args: nil})
	}
	es.Publish()
	if count != size {
		t.Errorf("overflow: got %d events; want %d", count, size)
	}
}

// TestOverflowReturnError ensures ReturnError policy fails fast.
func TestOverflowReturnError(t *testing.T) {
	dispatcher := Dispatcher{}
	es := NewEventStore(&dispatcher, 8, ReturnError) // small buffer
	for i := 0; i < 8; i++ {
		if err := es.Subscribe(bg, Event{ID: strconv.Itoa(i), Projection: "x", Args: nil}); err != nil {
			t.Fatalf("unexpected error pre-fill: %v", err)
		}
	}
	if err := es.Subscribe(bg, Event{ID: "9", Projection: "x", Args: nil}); err != ErrBufferFull {
		t.Errorf("expected ErrBufferFull; got %v", err)
	}
}

// TestConcurrentSubscribe ensures safety of concurrent subscriptions.
func TestConcurrentSubscribe(t *testing.T) {
	dispatcher := Dispatcher{}
	var count uint64
	dispatcher.Register("evt", func(_ context.Context, ev Event) (Result, error) {
		atomic.AddUint64(&count, 1)
		return Result{}, nil
	})
	es := NewEventStore(&dispatcher, 1<<16, DropOldest)
	var wg sync.WaitGroup
	const n = 1000
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			_ = es.Subscribe(bg, Event{ID: "c", Projection: "evt", Args: nil})
			wg.Done()
		}()
	}
	wg.Wait()
	es.Publish()

	if count != n {
		t.Errorf("concurrent subscribe: got %d events; want %d", count, n)
	}
}

// TestEventStore_ReturnErrorIsBoundedUnderContention verifies that concurrent
// producers can claim at most one slot each. A check-then-increment queue can
// overrun this boundary and silently overwrite accepted events.
func TestEventStore_ReturnErrorIsBoundedUnderContention(t *testing.T) {
	const (
		capacity  = 8
		producers = 64
		rounds    = 32
	)

	for round := 0; round < rounds; round++ {
		seen := make(map[int]struct{}, capacity)
		var seenMu sync.Mutex
		dispatcher := Dispatcher{}
		dispatcher.Register("event", func(_ context.Context, event Event) (Result, error) {
			seenMu.Lock()
			seen[event.Data.(int)] = struct{}{}
			seenMu.Unlock()
			return Result{}, nil
		})
		store := NewEventStore(&dispatcher, capacity, ReturnError)

		start := make(chan struct{})
		outcomes := make(chan error, producers)
		var producersWG sync.WaitGroup
		producersWG.Add(producers)
		for producer := 0; producer < producers; producer++ {
			producer := producer
			go func() {
				defer producersWG.Done()
				<-start
				outcomes <- store.Subscribe(context.Background(), Event{
					ID:         strconv.Itoa(producer),
					Projection: "event",
					Data:       producer,
				})
			}()
		}

		close(start)
		producersWG.Wait()
		close(outcomes)

		successes := 0
		for err := range outcomes {
			switch {
			case err == nil:
				successes++
			case errors.Is(err, ErrBufferFull):
			default:
				t.Fatalf("round %d: Subscribe error = %v", round, err)
			}
		}
		if successes != capacity {
			t.Fatalf("round %d: accepted %d events; want exactly %d", round, successes, capacity)
		}

		store.Publish()
		seenMu.Lock()
		got := len(seen)
		seenMu.Unlock()
		if got != capacity {
			t.Fatalf("round %d: dispatched %d distinct events; want %d", round, got, capacity)
		}
		if err := store.Close(context.Background()); err != nil {
			t.Fatalf("round %d: Close: %v", round, err)
		}
	}
}

// TestEventStore_MPMCStressExactlyOnce exercises slot reuse while producers
// and callers of Publish run concurrently. Every accepted event must reach a
// handler once, never twice.
func TestEventStore_MPMCStressExactlyOnce(t *testing.T) {
	const (
		producers         = 4
		eventsPerProducer = 256
		consumers         = 4
		total             = producers * eventsPerProducer
	)

	seen := make(map[int]uint8, total)
	var (
		seenMu    sync.Mutex
		duplicate bool
	)
	dispatcher := Dispatcher{}
	dispatcher.Register("event", func(_ context.Context, event Event) (Result, error) {
		id := event.Data.(int)
		seenMu.Lock()
		seen[id]++
		duplicate = duplicate || seen[id] > 1
		seenMu.Unlock()
		return Result{}, nil
	})
	store := NewEventStore(&dispatcher, 64, Block)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := make(chan struct{})
	producersDone := make(chan struct{})
	errs := make(chan error, producers)
	var producersWG sync.WaitGroup
	producersWG.Add(producers)
	for producer := 0; producer < producers; producer++ {
		producer := producer
		go func() {
			defer producersWG.Done()
			<-start
			for sequence := 0; sequence < eventsPerProducer; sequence++ {
				id := producer*eventsPerProducer + sequence
				if err := store.Subscribe(ctx, Event{
					ID:         strconv.Itoa(id),
					Projection: "event",
					Data:       id,
				}); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	go func() {
		producersWG.Wait()
		close(producersDone)
	}()

	var consumersWG sync.WaitGroup
	consumersWG.Add(consumers)
	for consumer := 0; consumer < consumers; consumer++ {
		go func() {
			defer consumersWG.Done()
			<-start
			for {
				store.Publish()
				select {
				case <-producersDone:
					store.Publish()
					return
				default:
				}
			}
		}()
	}

	close(start)
	consumersWG.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("Subscribe: %v", err)
	}
	store.Publish()

	seenMu.Lock()
	got, duplicated := len(seen), duplicate
	seenMu.Unlock()
	if got != total || duplicated {
		t.Fatalf("delivered %d/%d events, duplicate=%v", got, total, duplicated)
	}
	published, processed, errors := store.Metrics()
	if published != total || processed != total || errors != 0 {
		t.Fatalf("metrics = published:%d processed:%d errors:%d; want %d:%d:0", published, processed, errors, total, total)
	}
	if err := store.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestEventStore_CloseDrainsAcceptedEventsAndRejectsNewOnes(t *testing.T) {
	const eventCount = 32
	var processed atomic.Uint64
	dispatcher := Dispatcher{}
	dispatcher.Register("event", func(_ context.Context, _ Event) (Result, error) {
		processed.Add(1)
		return Result{}, nil
	})
	store := NewEventStore(&dispatcher, 64, DropOldest)
	store.Async = true
	for i := 0; i < eventCount; i++ {
		if err := store.Subscribe(context.Background(), Event{Projection: "event"}); err != nil {
			t.Fatalf("Subscribe(%d): %v", i, err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := store.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := processed.Load(); got != eventCount {
		t.Fatalf("processed %d events; want %d", got, eventCount)
	}
	if err := store.Subscribe(context.Background(), Event{Projection: "event"}); !errors.Is(err, ErrEventStoreClosed) {
		t.Fatalf("Subscribe after Close = %v; want ErrEventStoreClosed", err)
	}
}

func TestEventStore_CloseUnblocksBlockedSubscribe(t *testing.T) {
	dispatcher := Dispatcher{}
	store := NewEventStore(&dispatcher, 1, Block)
	if err := store.Subscribe(context.Background(), Event{}); err != nil {
		t.Fatalf("initial Subscribe: %v", err)
	}

	started := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		close(started)
		result <- store.Subscribe(context.Background(), Event{})
	}()
	<-started

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := store.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, ErrEventStoreClosed) {
			t.Fatalf("blocked Subscribe = %v; want ErrEventStoreClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked Subscribe did not return after Close")
	}
}

func TestEventStore_OrderedAsyncPreservesOrderAcrossConcurrentPublish(t *testing.T) {
	type orderedEvent struct{ sequence int }

	firstKeyEntered := make(chan struct{})
	releaseFirstKey := make(chan struct{})
	var (
		sequences []int
		mu        sync.Mutex
	)
	dispatcher := Dispatcher{}
	store := NewEventStore(&dispatcher, 4, Block)
	store.Async = true
	store.RegisterOrdered("ordered", func(event Event) string {
		if event.Data.(orderedEvent).sequence == 0 {
			close(firstKeyEntered)
			<-releaseFirstKey
		}
		return "same-key"
	}, func(_ context.Context, event Event) (Result, error) {
		mu.Lock()
		sequences = append(sequences, event.Data.(orderedEvent).sequence)
		mu.Unlock()
		return Result{}, nil
	})

	if err := store.Subscribe(context.Background(), Event{Projection: "ordered", Data: orderedEvent{sequence: 0}}); err != nil {
		t.Fatalf("Subscribe(first): %v", err)
	}
	firstPublishDone := make(chan struct{})
	go func() {
		store.Publish()
		close(firstPublishDone)
	}()
	<-firstKeyEntered

	if err := store.Subscribe(context.Background(), Event{Projection: "ordered", Data: orderedEvent{sequence: 1}}); err != nil {
		t.Fatalf("Subscribe(second): %v", err)
	}
	secondPublishDone := make(chan struct{})
	go func() {
		store.Publish()
		close(secondPublishDone)
	}()

	// The second Publish must not enqueue its later event while the first
	// publisher is still choosing the ordering key for the earlier event.
	secondOvertookFirst := false
	select {
	case <-secondPublishDone:
		secondOvertookFirst = true
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseFirstKey)
	<-firstPublishDone
	<-secondPublishDone

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := store.Drain(ctx); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	mu.Lock()
	got := append([]int(nil), sequences...)
	mu.Unlock()
	if secondOvertookFirst || len(got) != 2 || got[0] != 0 || got[1] != 1 {
		t.Fatalf("ordered delivery = %v, later Publish overtook first = %v", got, secondOvertookFirst)
	}
}

// Benchmarks --------------------------------------------------------------

func BenchmarkSubscribe(b *testing.B) {
	dispatcher := Dispatcher{"evt": hs(func(_ context.Context, ev Event) (Result, error) { return Result{}, nil })}
	es := NewEventStore(&dispatcher, 1<<16, DropOldest)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = es.Subscribe(bg, Event{ID: "bench", Projection: "evt", Args: nil})
	}
}

func BenchmarkSubscribeParallel(b *testing.B) {
	dispatcher := Dispatcher{"evt": hs(func(_ context.Context, ev Event) (Result, error) { return Result{}, nil })}
	es := NewEventStore(&dispatcher, 1<<16, DropOldest)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = es.Subscribe(bg, Event{ID: "pp", Projection: "evt", Args: nil})
		}
	})
}

func BenchmarkPublish(b *testing.B) {
	dispatcher := Dispatcher{"evt": hs(func(_ context.Context, ev Event) (Result, error) { return Result{}, nil })}
	es := NewEventStore(&dispatcher, 1<<16, DropOldest)
	for i := 0; i < size; i++ {
		_ = es.Subscribe(bg, Event{ID: "p", Projection: "evt", Args: nil})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		es.Publish()
	}
}

func BenchmarkPublishAfterPrefill(b *testing.B) {
	dispatcher := Dispatcher{"evt": hs(func(_ context.Context, ev Event) (Result, error) { return Result{}, nil })}
	es := NewEventStore(&dispatcher, 1<<16, DropOldest)
	for i := 0; i < size; i++ {
		_ = es.Subscribe(bg, Event{ID: "pp", Projection: "evt", Args: nil})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		es.Publish()
	}
}

// Large payload benchmarks remain mostly unchanged but updated API

// LargeStruct is a sample struct for payload-heavy benchmarks.
type LargeStruct struct {
	Data [1024]byte
}

func BenchmarkSubscribeLargePayload(b *testing.B) {
	dispatcher := Dispatcher{"evtLarge": hs(func(_ context.Context, ev Event) (Result, error) { return Result{}, nil })}
	es := NewEventStore(&dispatcher, 1<<16, DropOldest)
	var payload LargeStruct

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = es.Subscribe(bg, Event{ID: "bench", Projection: "evtLarge", Args: map[string]any{"payload": payload}})
	}
}

func BenchmarkPublishLargePayload(b *testing.B) {
	dispatcher := Dispatcher{"evtLarge": hs(func(_ context.Context, ev Event) (Result, error) { return Result{}, nil })}
	es := NewEventStore(&dispatcher, 1<<16, DropOldest)
	var payload LargeStruct
	const largeSize = 100
	for i := 0; i < largeSize; i++ {
		_ = es.Subscribe(bg, Event{ID: "bench", Projection: "evtLarge", Args: map[string]any{"payload": payload}})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		es.Publish()
	}
}

// Additional tests for exact buffer behaviour
func TestExactBufferSizeNoOverflow(t *testing.T) {
	dispatcher := Dispatcher{}
	var count uint64
	dispatcher.Register("evtExact", func(_ context.Context, ev Event) (Result, error) {
		atomic.AddUint64(&count, 1)
		return Result{}, nil
	})
	es := NewEventStore(&dispatcher, 1<<16, DropOldest)
	for i := 0; i < size; i++ {
		_ = es.Subscribe(bg, Event{ID: strconv.Itoa(i), Projection: "evtExact", Args: nil})
	}
	es.Publish()
	if count != size {
		t.Errorf("no-overflow: got %d calls; want %d", count, size)
	}
}

func TestOverflowThreshold(t *testing.T) {
	dispatcher := Dispatcher{}
	var count uint64
	dispatcher.Register("evtThresh", func(_ context.Context, ev Event) (Result, error) {
		atomic.AddUint64(&count, 1)
		return Result{}, nil
	})
	es := NewEventStore(&dispatcher, 1<<16, DropOldest)
	for i := 0; i < size+1; i++ {
		_ = es.Subscribe(bg, Event{ID: strconv.Itoa(i), Projection: "evtThresh", Args: nil})
	}
	es.Publish()
	if count != size {
		t.Errorf("threshold-overflow: got %d calls; want %d", count, size)
	}
}

func TestPublishIdempotent(t *testing.T) {
	dispatcher := Dispatcher{}
	var called int32
	dispatcher.Register("evtOnce", func(_ context.Context, ev Event) (Result, error) {
		atomic.AddInt32(&called, 1)
		return Result{}, nil
	})
	es := NewEventStore(&dispatcher, 1<<16, DropOldest)
	_ = es.Subscribe(bg, Event{ID: "1", Projection: "evtOnce", Args: nil})
	es.Publish()
	es.Publish()
	if called != 1 {
		t.Errorf("idempotent publish: got %d calls; want 1", called)
	}
}

func TestEventStore_AsyncDispatch(t *testing.T) {
	var mu sync.Mutex
	called := 0

	dispatcher := Dispatcher{
		"print": hs(func(_ context.Context, ev Event) (Result, error) {
			mu.Lock()
			defer mu.Unlock()
			called++
			return Result{Message: "ok"}, nil
		}),
	}

	store := NewEventStore(&dispatcher, 1<<16, DropOldest)
	store.Async = true

	for i := 0; i < 10; i++ {
		_ = store.Subscribe(bg, Event{ID: "e1", Projection: "print", Args: map[string]any{"data": i}})
	}

	store.Publish()
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if called != 10 {
		t.Errorf("Expected 10 calls, got %d", called)
	}
}

func TestEventStore_OrderedAsyncPreservesOrderPerKey(t *testing.T) {
	type orderEvent struct {
		OrderID  string
		Sequence int
	}

	dispatcher := Dispatcher{}
	store := NewEventStore(&dispatcher, 1<<16, DropOldest)
	store.Async = true

	var (
		mu        sync.Mutex
		sequences = make(map[string][]int)
	)
	store.RegisterOrdered("order", func(ev Event) string {
		return ev.Data.(orderEvent).OrderID
	}, func(_ context.Context, ev Event) (Result, error) {
		data := ev.Data.(orderEvent)
		// Without ordered delivery, later events would be able to complete
		// while the first event is still blocked.
		if data.Sequence == 0 {
			time.Sleep(25 * time.Millisecond)
		}
		mu.Lock()
		sequences[data.OrderID] = append(sequences[data.OrderID], data.Sequence)
		mu.Unlock()
		return Result{}, nil
	})

	const count = 40
	for i := 0; i < count; i++ {
		if err := store.Subscribe(bg, Event{
			Projection: "order",
			Data:       orderEvent{OrderID: "order-a", Sequence: i},
		}); err != nil {
			t.Fatalf("Subscribe(order-a): %v", err)
		}
		if err := store.Subscribe(bg, Event{
			Projection: "order",
			Data:       orderEvent{OrderID: "order-b", Sequence: i},
		}); err != nil {
			t.Fatalf("Subscribe(order-b): %v", err)
		}
	}

	store.Publish()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := store.Drain(ctx); err != nil {
		t.Fatalf("Drain: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	for _, orderID := range []string{"order-a", "order-b"} {
		got := sequences[orderID]
		if len(got) != count {
			t.Fatalf("%s: handled %d events; want %d", orderID, len(got), count)
		}
		for i, sequence := range got {
			if sequence != i {
				t.Fatalf("%s: event at index %d has sequence %d; want %d", orderID, i, sequence, i)
			}
		}
	}
}

func TestEventStore_OrderedHandlerUsesMiddlewareAndHooks(t *testing.T) {
	dispatcher := Dispatcher{}
	store := NewEventStore(&dispatcher, 8, DropOldest)

	var before, after, middleware, handler int
	store.OnBefore(func(context.Context, Event) { before++ })
	store.OnAfter(func(context.Context, Event, Result, error) { after++ })
	store.Use(func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, ev Event) (Result, error) {
			middleware++
			return next(ctx, ev)
		}
	})
	store.RegisterOrdered("ordered", func(Event) string { return "key" }, func(context.Context, Event) (Result, error) {
		handler++
		return Result{}, nil
	})

	if err := store.Subscribe(bg, Event{Projection: "ordered"}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	store.Publish()

	if before != 1 || after != 1 || middleware != 1 || handler != 1 {
		t.Fatalf("before=%d after=%d middleware=%d handler=%d; want each 1", before, after, middleware, handler)
	}
}

func BenchmarkEventStore_Async(b *testing.B) {
	dispatcher := Dispatcher{"async": hs(func(_ context.Context, ev Event) (Result, error) { return Result{Message: "done"}, nil })}
	store := NewEventStore(&dispatcher, 1<<16, DropOldest)
	store.Async = true
	for i := 0; i < b.N; i++ {
		_ = store.Subscribe(bg, Event{ID: "event", Projection: "async", Args: map[string]any{"n": i}})
	}
	store.Publish()
}

func BenchmarkEventStore_Sync(b *testing.B) {
	dispatcher := Dispatcher{"sync": hs(func(_ context.Context, ev Event) (Result, error) { return Result{Message: "done"}, nil })}
	store := NewEventStore(&dispatcher, 1<<16, DropOldest)
	store.Async = false
	for i := 0; i < b.N; i++ {
		_ = store.Subscribe(bg, Event{ID: "event", Projection: "sync", Args: map[string]any{"n": i}})
	}
	store.Publish()
}

// FastHTTP benchmarks updated for new API
func benchmarkFastHTTP(b *testing.B, async bool) {
	dispatcher := Dispatcher{"evt": hs(func(_ context.Context, ev Event) (Result, error) { return Result{}, nil })}
	es := NewEventStore(&dispatcher, 1<<16, DropOldest)
	es.Async = async

	handler := func(ctx *fasthttp.RequestCtx) {
		_ = es.Subscribe(bg, Event{ID: "bench", Projection: "evt", Args: nil})
		es.Publish()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var ctx fasthttp.RequestCtx
		handler(&ctx)
	}
}

func BenchmarkFastHTTPSync(b *testing.B)  { benchmarkFastHTTP(b, false) }
func BenchmarkFastHTTPAsync(b *testing.B) { benchmarkFastHTTP(b, true) }

func BenchmarkFastHTTPParallel(b *testing.B) {
	dispatcher := Dispatcher{"evt": hs(func(_ context.Context, ev Event) (Result, error) { return Result{}, nil })}
	es := NewEventStore(&dispatcher, 1<<16, DropOldest)
	es.Async = true

	handler := func(ctx *fasthttp.RequestCtx) {
		_ = es.Subscribe(bg, Event{ID: "bench", Projection: "evt", Args: nil})
		es.Publish()
	}

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			var ctx fasthttp.RequestCtx
			handler(&ctx)
		}
	})
}

// TestPublishEmpty ensures Publish on empty store does nothing
func TestPublishEmpty(t *testing.T) {
	dispatcher := Dispatcher{}
	es := NewEventStore(&dispatcher, 1<<16, DropOldest)
	initialPublished, initialProcessed, initialErrors := es.Metrics()
	es.Publish()
	published, processed, errors := es.Metrics()
	if published != initialPublished || processed != initialProcessed || errors != initialErrors {
		t.Errorf("metrics changed: published %d->%d processed %d->%d errors %d->%d", initialPublished, published, initialProcessed, processed, initialErrors, errors)
	}
}

// TestArgsPassing ensures arguments are passed through
func TestArgsPassing(t *testing.T) {
	dispatcher := Dispatcher{}
	var received string
	dispatcher.Register("echo", func(_ context.Context, ev Event) (Result, error) {
		received = ev.Args["foo"].(string)
		return Result{}, nil
	})
	es := NewEventStore(&dispatcher, 1<<16, DropOldest)
	_ = es.Subscribe(bg, Event{ID: "1", Projection: "echo", Args: map[string]any{"foo": "bar"}})
	es.Publish()
	if received != "bar" {
		t.Errorf("expected 'bar'; got '%s'", received)
	}
}

func TestDispatcherSnapshot(t *testing.T) {
	dispatcher := Dispatcher{}
	var calledOriginal, calledModified int32
	dispatcher.Register("snap", func(_ context.Context, ev Event) (Result, error) {
		atomic.AddInt32(&calledOriginal, 1)
		return Result{}, nil
	})
	es := NewEventStore(&dispatcher, 1<<16, DropOldest)
	// swap handler slice
	dispatcher["snap"] = hs(func(_ context.Context, ev Event) (Result, error) {
		atomic.AddInt32(&calledModified, 1)
		return Result{}, nil
	})
	_ = es.Subscribe(bg, Event{ID: "1", Projection: "snap", Args: nil})
	es.Publish()
	if calledOriginal != 0 {
		t.Errorf("original handler called %d", calledOriginal)
	}
	if calledModified != 1 {
		t.Errorf("modified handler should be called once; got %d", calledModified)
	}
}

func TestEventStore_Metrics(t *testing.T) {
	dispatcher := Dispatcher{"metric": hs(func(_ context.Context, ev Event) (Result, error) { return Result{}, nil })}
	es := NewEventStore(&dispatcher, 1<<16, DropOldest)
	_ = es.Subscribe(bg, Event{ID: "1", Projection: "metric", Args: nil})
	_ = es.Subscribe(bg, Event{ID: "2", Projection: "metric", Args: nil})

	published, processed, errors := es.Metrics()
	if published != 2 || processed != 0 || errors != 0 {
		t.Fatalf("before publish: got %d %d %d", published, processed, errors)
	}
	es.Publish()
	published, processed, errors = es.Metrics()
	if published != 2 || processed != 2 || errors != 0 {
		t.Fatalf("after publish: got %d %d %d", published, processed, errors)
	}
}

// helper no‑op handler that increments a counter so we know it was invoked.
func noopHandler(counter *uint64) func(context.Context, Event) (Result, error) {
	return func(ctx context.Context, ev Event) (Result, error) {
		atomic.AddUint64(counter, 1)
		return Result{Message: "ok"}, nil
	}
}

// TestMetricsCounts ensures that the published/processed/error counters are accurate.
func TestMetricsCounts(t *testing.T) {
	var invoked uint64
	disp := Dispatcher{"test": hs(noopHandler(&invoked))}
	es := NewEventStore(&disp, 8, DropOldest)

	// publish 5 successful events
	for i := 0; i < 5; i++ {
		if err := es.Subscribe(context.Background(), Event{ID: "e", Projection: "test", Args: map[string]any{}}); err != nil {
			t.Fatalf("unexpected Subscribe error: %v", err)
		}
	}

	es.Publish()

	pub, proc, errCnt := es.Metrics()
	if pub != 5 || proc != 5 || errCnt != 0 {
		t.Fatalf("unexpected metrics: published=%d processed=%d errors=%d", pub, proc, errCnt)
	}
	if atomic.LoadUint64(&invoked) != 5 {
		t.Fatalf("handler invoked %d times, want 5", invoked)
	}
}

// TestOverrunPolicyReturnError verifies that Subscribe returns ErrBufferFull
// when the buffer is full and policy is ReturnError.
func TestOverrunPolicyReturnError(t *testing.T) {
	disp := Dispatcher{}
	es := NewEventStore(&disp, 2, ReturnError)

	// fill buffer
	_ = es.Subscribe(context.Background(), Event{})
	_ = es.Subscribe(context.Background(), Event{})

	if err := es.Subscribe(context.Background(), Event{}); err != ErrBufferFull {
		t.Fatalf("expected ErrBufferFull, got %v", err)
	}
}

// BenchmarkPublish measures throughput of synchronous vs asynchronous Publish.
func BenchmarkSubscribePublish(b *testing.B) {
	bench := func(async bool) {
		var invoked uint64
		disp := Dispatcher{"bench": hs(noopHandler(&invoked))}
		es := NewEventStore(&disp, 1024, DropOldest)
		es.Async = async
		ctx := context.Background()

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = es.Subscribe(ctx, Event{Projection: "bench"})
			es.Publish()
		}
		b.StopTimer()

		// wait for async goroutines to finish to avoid leaking
		if async {
			time.Sleep(10 * time.Millisecond)
		}
	}

	b.Run("Sync", func(b *testing.B) { bench(false) })
	b.Run("Async", func(b *testing.B) { bench(true) })
}

// TestContextPropagation verifies that a context passed to Subscribe
// is forwarded to the handler.
func TestContextPropagation(t *testing.T) {
	type contextKey struct{}
	const val = "myVal"
	key := contextKey{}

	ctx := context.WithValue(context.Background(), key, val)

	var received string
	disp := Dispatcher{
		"ctx": hs(func(c context.Context, ev Event) (Result, error) {
			if v, ok := c.Value(key).(string); ok {
				received = v
			}
			return Result{}, nil
		}),
	}

	es := NewEventStore(&disp, 8, DropOldest)
	_ = es.Subscribe(ctx, Event{ID: "1", Projection: "ctx"})
	es.Publish()

	if received != val {
		t.Fatalf("context value mismatch: want %q got %q", val, received)
	}
}

// TestOverrunPolicyBlockRespectsContext ensures that Subscribe returns the
// caller's context error when the buffer remains full beyond the deadline.
func TestOverrunPolicyBlockRespectsContext(t *testing.T) {
	disp := Dispatcher{}
	es := NewEventStore(&disp, 2, Block) // intentionally small buffer

	// pre‑fill to capacity so subsequent Subscribe must block
	_ = es.Subscribe(context.Background(), Event{})
	_ = es.Subscribe(context.Background(), Event{})

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := es.Subscribe(ctx, Event{})
	elapsed := time.Since(start)

	if err == nil {
		t.Fatalf("expected error, got nil")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context deadline exceeded, got %v", err)
	}
	if elapsed < 40*time.Millisecond {
		t.Fatalf("Subscribe returned too early: elapsed %v < ctx timeout", elapsed)
	}
}

// TestErrorMetrics verifies that handler failures are reflected in Metrics().
func TestErrorMetrics(t *testing.T) {
	disp := Dispatcher{"boom": hs(func(_ context.Context, ev Event) (Result, error) {
		return Result{}, errors.New("boom")
	})}
	es := NewEventStore(&disp, 4, DropOldest)

	_ = es.Subscribe(context.Background(), Event{ID: "e", Projection: "boom"})
	es.Publish()

	pub, proc, errs := es.Metrics()
	if pub != 1 || proc != 1 || errs != 1 {
		t.Fatalf("unexpected metrics – published=%d processed=%d errors=%d", pub, proc, errs)
	}
}

// -----------------------------------------------------------------------------
// Micro‑benchmarks for Block policy and error‑heavy workloads.
// -----------------------------------------------------------------------------

func BenchmarkSubscribeBlockPolicy(b *testing.B) {
	disp := Dispatcher{}
	es := NewEventStore(&disp, 1024, Block)

	// Fill the buffer to force Subscribe to exercise the Block path.
	for i := 0; i < 1024; i++ {
		_ = es.Subscribe(context.Background(), Event{})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// use an already‑expired context so the call returns immediately via the Block path
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_ = es.Subscribe(ctx, Event{})
	}
}

func BenchmarkPublishWithErrors(b *testing.B) {
	disp := Dispatcher{"err": hs(func(_ context.Context, ev Event) (Result, error) { return Result{}, errors.New("fail") })}
	es := NewEventStore(&disp, 1<<16, DropOldest)

	// pre‑populate with events that will all fail
	for i := 0; i < 1<<16; i++ {
		_ = es.Subscribe(context.Background(), Event{ID: "e", Projection: "err"})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		es.Publish()
	}
}

// Test basic Subscribe and Publish functionality in synchronous mode.
func TestEventStore_SubscribePublish_Sync(t *testing.T) {
	disp := Dispatcher{}
	// simple echo handler
	disp.Register("echo", func(ctx context.Context, ev Event) (Result, error) {
		return Result{Message: ev.Args["msg"].(string)}, nil
	})
	store := NewEventStore(&disp, 8, DropOldest)
	e := Event{ID: "1", Projection: "echo", Args: map[string]any{"msg": "hello"}}
	if err := store.Subscribe(context.Background(), e); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}
	store.Publish()
	pub, proc, errs := store.Metrics()
	if pub != 1 || proc != 1 || errs != 0 {
		t.Errorf("Metrics mismatch: published=%d, processed=%d, errors=%d", pub, proc, errs)
	}
}

// Test middleware chaining.
func TestEventStore_Middleware(t *testing.T) {
	disp := Dispatcher{}
	disp.Register("inc", func(ctx context.Context, ev Event) (Result, error) {
		return Result{Message: ""}, nil
	})
	store := NewEventStore(&disp, 8, DropOldest)
	// middleware increments counter before handler
	store.Use(func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, ev Event) (Result, error) {
			ev.Args["cnt"] = ev.Args["cnt"].(int) + 1
			return next(ctx, ev)
		}
	})
	e := Event{ID: "1", Projection: "inc", Args: map[string]any{"cnt": 0}}
	if err := store.Subscribe(context.Background(), e); err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}
	store.Publish()
	// after middleware, cnt should be 1
	if e.Args["cnt"].(int) != 1 {
		t.Errorf("Middleware did not run, cnt=%v", e.Args["cnt"])
	}
}

// Test hooks invocation order and error hooks.
func TestEventStore_Hooks(t *testing.T) {
	disp := Dispatcher{}
	errorMsg := "handler error"
	disp.Register("fail", func(ctx context.Context, ev Event) (Result, error) {
		return Result{}, errors.New(errorMsg)
	})
	store := NewEventStore(&disp, 8, DropOldest)

	var beforeCalled, afterCalled, errorCalled atomic.Bool

	store.OnBefore(func(ctx context.Context, ev Event) {
		beforeCalled.Store(true)
	})
	store.OnAfter(func(ctx context.Context, ev Event, res Result, err error) {
		afterCalled.Store(true)
	})
	store.OnError(func(ctx context.Context, ev Event, err error) {
		errorCalled.Store(true)
	})

	e := Event{ID: "1", Projection: "fail", Args: map[string]any{}}
	_ = store.Subscribe(context.Background(), e)
	store.Publish()

	if !beforeCalled.Load() {
		t.Error("Before hook not called")
	}
	if !afterCalled.Load() {
		t.Error("After hook not called")
	}
	if !errorCalled.Load() {
		t.Error("Error hook not called")
	}
}

func TestEventStore_SubscribePublishDrainMetrics(t *testing.T) {
	var processed atomic.Uint64
	dispatcher := Dispatcher{
		"testEvent": hs(func(ctx context.Context, ev Event) (Result, error) {
			processed.Add(1)
			return Result{Message: "ok"}, nil
		}),
	}
	store := NewEventStore(&dispatcher, 16, DropOldest)
	store.Async = true

	// Subscribe multiple events
	for i := 0; i < 5; i++ {
		err := store.Subscribe(context.Background(), Event{
			ID:         "evt" + strconv.Itoa(i),
			Projection: "testEvent",
			Args:       nil,
		})
		if err != nil {
			t.Fatalf("Subscribe failed: %v", err)
		}
	}

	// Check metrics after Subscribe
	published, processedCount, errorCount := store.Metrics()
	if published != 5 || processedCount != 0 || errorCount != 0 {
		t.Errorf("after subscribe: published=%d processed=%d errors=%d", published, processedCount, errorCount)
	}

	// Publish events
	store.Publish()

	// Drain to wait for async handlers to finish
	if err := store.Drain(context.Background()); err != nil {
		t.Fatalf("Drain failed: %v", err)
	}

	// Check final metrics
	published, processedCount, errorCount = store.Metrics()
	if published != 5 || processedCount != 5 || errorCount != 0 {
		t.Errorf("after drain: published=%d processed=%d errors=%d", published, processedCount, errorCount)
	}

	// Check processed counter
	if processed.Load() != 5 {
		t.Errorf("expected 5 events processed, got %d", processed.Load())
	}
}

// noOpHandler is a dummy handler for benchmarks.
func noOpHandler(ctx context.Context, ev Event) (Result, error) {
	return Result{}, nil
}

// setupEventStore initializes an EventStore with the given async flag and buffer size.
func setupEventStore(async bool, bufferSize uint64) *EventStore {
	disp := Dispatcher{"test": hs(noOpHandler)}
	es := NewEventStore(&disp, bufferSize, DropOldest)
	es.Async = async
	return es
}

// BenchmarkDrainSync measures the performance of Drain on a synchronous EventStore.
func BenchmarkDrainSync(b *testing.B) {
	// Prepare a store with a batch of events
	es := setupEventStore(false, 1024)
	for i := 0; i < 1000; i++ {
		es.Subscribe(context.Background(), Event{Projection: "test", Args: map[string]any{}})
	}
	es.Publish()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// In sync mode, Drain should return immediately (no-op)
		es.Drain(context.Background())
	}
}

// BenchmarkDrainAsync measures the performance of Drain on an asynchronous EventStore.
// It uses StopTimer/StartTimer to exclude setup work (subscribe and publish) from timing.
func BenchmarkDrainAsync(b *testing.B) {
	for i := 0; i < b.N; i++ {
		// Setup a fresh async store
		es := setupEventStore(true, 1024)

		// Enqueue events and publish outside timed section
		b.StopTimer()
		for j := 0; j < 1000; j++ {
			es.Subscribe(context.Background(), Event{Projection: "test", Args: map[string]any{}})
		}
		es.Publish()

		// Time only the Drain call
		b.StartTimer()
		es.Drain(context.Background())
	}
}

func TestScheduleAfter(t *testing.T) {
	var called uint32
	dispatcher := Dispatcher{
		"foo": hs(func(ctx context.Context, ev Event) (Result, error) {
			atomic.StoreUint32(&called, 1)
			return Result{}, nil
		}),
	}
	es := NewEventStore(&dispatcher, 8, DropOldest)
	// schedule 50ms in future
	es.ScheduleAfter(context.Background(), 50*time.Millisecond, Event{Projection: "foo"})
	time.Sleep(100 * time.Millisecond)
	if atomic.LoadUint32(&called) != 1 {
		t.Fatal("expected handler to fire once")
	}
}

// TestScheduleAfter_FiresOnce verifies ScheduleAfter enqueues and publishes the event exactly once.
func TestScheduleAfter_FiresOnce(t *testing.T) {
	var called uint32
	dispatcher := Dispatcher{
		"foo": hs(func(ctx context.Context, ev Event) (Result, error) {
			atomic.StoreUint32(&called, 1)
			return Result{}, nil
		}),
	}
	es := NewEventStore(&dispatcher, 8, DropOldest)
	es.Async = true
	es.ScheduleAfter(context.Background(), 50*time.Millisecond, Event{Projection: "foo"})
	time.Sleep(100 * time.Millisecond)
	if atomic.LoadUint32(&called) != 1 {
		t.Fatal("expected handler to fire once")
	}
}

// TestSchedule_FiresImmediatelyIfPast checks that Schedule executes immediately when given a past time.
func TestSchedule_FiresImmediatelyIfPast(t *testing.T) {
	var called uint32
	dispatcher := Dispatcher{
		"bar": hs(func(ctx context.Context, ev Event) (Result, error) {
			atomic.StoreUint32(&called, 1)
			return Result{}, nil
		}),
	}
	es := NewEventStore(&dispatcher, 8, DropOldest)
	es.Async = true
	past := time.Now().Add(-time.Second)
	es.Schedule(context.Background(), past, Event{Projection: "bar"})
	if atomic.LoadUint32(&called) != 1 {
		t.Fatal("expected handler to fire immediately for past time")
	}
}

// TestSchedule_FiresAtFutureTime ensures the handler is not called before the scheduled time and fires shortly after.
func TestSchedule_FiresAtFutureTime(t *testing.T) {
	var called uint32
	dispatcher := Dispatcher{
		"baz": hs(func(ctx context.Context, ev Event) (Result, error) {
			atomic.StoreUint32(&called, 1)
			return Result{}, nil
		}),
	}
	es := NewEventStore(&dispatcher, 8, DropOldest)
	es.Async = true
	start := time.Now().Add(50 * time.Millisecond)
	es.Schedule(context.Background(), start, Event{Projection: "baz"})
	// Should not fire immediately
	if atomic.LoadUint32(&called) != 0 {
		t.Fatal("handler fired too early")
	}
	time.Sleep(100 * time.Millisecond)
	if atomic.LoadUint32(&called) != 1 {
		t.Fatal("expected handler to fire at scheduled time")
	}
}

// TestSchedule_Cancel verifies that stopping the timer prevents the event from firing.
func TestSchedule_Cancel(t *testing.T) {
	var called uint32
	dispatcher := Dispatcher{
		"qux": hs(func(ctx context.Context, ev Event) (Result, error) {
			atomic.StoreUint32(&called, 1)
			return Result{}, nil
		}),
	}
	es := NewEventStore(&dispatcher, 8, DropOldest)
	es.Async = true
	timer := es.ScheduleAfter(context.Background(), 50*time.Millisecond, Event{Projection: "qux"})
	if stopped := timer.Stop(); !stopped {
		t.Fatal("expected timer to stop successfully")
	}
	time.Sleep(100 * time.Millisecond)
	if atomic.LoadUint32(&called) != 0 {
		t.Fatal("handler fired despite cancellation")
	}
}

// TestWorkerRecoverFromPanic verifies that a panicking handler does not kill the worker
// goroutine and that subsequent events are still processed.
func TestWorkerRecoverFromPanic(t *testing.T) {
	var good uint32
	disp := Dispatcher{
		"panic": hs(func(_ context.Context, ev Event) (Result, error) {
			panic("intentional panic")
		}),
		"ok": hs(func(_ context.Context, ev Event) (Result, error) {
			atomic.AddUint32(&good, 1)
			return Result{}, nil
		}),
	}
	es := NewEventStore(&disp, 16, DropOldest)
	es.Async = true

	// publish a panicking event followed by a good event
	_ = es.Subscribe(context.Background(), Event{Projection: "panic"})
	_ = es.Subscribe(context.Background(), Event{Projection: "ok"})
	es.Publish()

	drainCtx, drainCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer drainCancel()
	if err := es.Drain(drainCtx); err != nil {
		t.Fatalf("Drain failed: %v", err)
	}
	if atomic.LoadUint32(&good) != 1 {
		t.Fatalf("expected good handler to run once; got %d", good)
	}
	_, _, errs := es.Metrics()
	if errs != 1 {
		t.Fatalf("expected error counter == 1 (for the panic); got %d", errs)
	}
}

// TestFanOut verifies that multiple handlers registered for the same projection
// are all invoked in order for each dispatched event.
func TestFanOut(t *testing.T) {
	var mu sync.Mutex
	var order []string

	disp := Dispatcher{}
	disp.Register("evt",
		func(_ context.Context, ev Event) (Result, error) {
			mu.Lock()
			order = append(order, "A")
			mu.Unlock()
			return Result{Message: "A"}, nil
		},
		func(_ context.Context, ev Event) (Result, error) {
			mu.Lock()
			order = append(order, "B")
			mu.Unlock()
			return Result{Message: "B"}, nil
		},
		func(_ context.Context, ev Event) (Result, error) {
			mu.Lock()
			order = append(order, "C")
			mu.Unlock()
			return Result{Message: "C"}, nil
		},
	)

	es := NewEventStore(&disp, 8, DropOldest)
	_ = es.Subscribe(context.Background(), Event{ID: "1", Projection: "evt"})
	es.Publish()

	mu.Lock()
	defer mu.Unlock()
	if len(order) != 3 {
		t.Fatalf("expected 3 handler invocations; got %d (%v)", len(order), order)
	}
	if order[0] != "A" || order[1] != "B" || order[2] != "C" {
		t.Errorf("unexpected invocation order: %v", order)
	}
	_, processed, _ := es.Metrics()
	if processed != 3 {
		t.Errorf("expected processedCount==3; got %d", processed)
	}
}

// TestFanOut_AllHandlersRunOnError verifies that a failing handler does not
// prevent subsequent fan-out handlers from running (each subscriber is independent).
func TestFanOut_AllHandlersRunOnError(t *testing.T) {
	var calls []string
	disp := Dispatcher{}
	disp.Register("evt",
		func(_ context.Context, ev Event) (Result, error) {
			calls = append(calls, "A")
			return Result{}, errors.New("A failed")
		},
		func(_ context.Context, ev Event) (Result, error) {
			calls = append(calls, "B")
			return Result{}, nil
		},
	)

	es := NewEventStore(&disp, 8, DropOldest)
	_ = es.Subscribe(context.Background(), Event{ID: "1", Projection: "evt"})
	es.Publish()

	if len(calls) != 2 {
		t.Fatalf("expected both handlers to run; got %v", calls)
	}
	_, _, errs := es.Metrics()
	if errs != 1 {
		t.Errorf("expected 1 error; got %d", errs)
	}
}

// TestFanOut_Async verifies fan-out works in async mode.
func TestFanOut_Async(t *testing.T) {
	var count atomic.Int32
	disp := Dispatcher{}
	disp.Register("evt",
		func(_ context.Context, ev Event) (Result, error) { count.Add(1); return Result{}, nil },
		func(_ context.Context, ev Event) (Result, error) { count.Add(1); return Result{}, nil },
		func(_ context.Context, ev Event) (Result, error) { count.Add(1); return Result{}, nil },
	)

	es := NewEventStore(&disp, 16, DropOldest)
	es.Async = true

	for i := 0; i < 4; i++ {
		_ = es.Subscribe(context.Background(), Event{ID: strconv.Itoa(i), Projection: "evt"})
	}
	es.Publish()

	drainCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := es.Drain(drainCtx); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	// 4 events x 3 handlers each = 12
	if count.Load() != 12 {
		t.Errorf("expected 12 handler calls; got %d", count.Load())
	}
}

// TestNewEventStore_NilDispatcherPanics verifies fail-fast on nil dispatcher.
func TestNewEventStore_NilDispatcherPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for nil dispatcher, got none")
		}
	}()
	NewEventStore(nil, 8, DropOldest)
}

// TestNewEventStore_ZeroBufferPanics verifies fail-fast on zero buffer size.
func TestNewEventStore_ZeroBufferPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for zero buffer size, got none")
		}
	}()
	disp := Dispatcher{}
	NewEventStore(&disp, 0, DropOldest)
}
