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

// TestSubscribeAndPublish verifies that events are stored and published correctly.
func TestSubscribeAndPublish(t *testing.T) {
	dispatcher := Dispatcher{}
	var called1, called2 int32
	dispatcher["evt1"] = func(_ context.Context, args map[string]any) (Result, error) {
		atomic.AddInt32(&called1, 1)
		return Result{Message: "ok1"}, nil
	}
	dispatcher["evt2"] = func(_ context.Context, args map[string]any) (Result, error) {
		atomic.AddInt32(&called2, 1)
		return Result{Message: "ok2"}, nil
	}

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
	dispatcher["evt"] = func(_ context.Context, args map[string]any) (Result, error) {
		atomic.AddInt32(&called, 1)
		return Result{}, nil
	}
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
	dispatcher["evtOverflow"] = func(_ context.Context, args map[string]any) (Result, error) {
		atomic.AddUint64(&count, 1)
		return Result{}, nil
	}
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
	dispatcher := Dispatcher{"evt": func(_ context.Context, args map[string]any) (Result, error) { return Result{}, nil }}
	var count uint64
	dispatcher["evt"] = func(_ context.Context, args map[string]any) (Result, error) {
		atomic.AddUint64(&count, 1)
		return Result{}, nil
	}
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

// Benchmarks --------------------------------------------------------------

func BenchmarkSubscribe(b *testing.B) {
	dispatcher := Dispatcher{"evt": func(_ context.Context, args map[string]any) (Result, error) { return Result{}, nil }}
	es := NewEventStore(&dispatcher, 1<<16, DropOldest)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = es.Subscribe(bg, Event{ID: "bench", Projection: "evt", Args: nil})
	}
}

func BenchmarkSubscribeParallel(b *testing.B) {
	dispatcher := Dispatcher{"evt": func(_ context.Context, args map[string]any) (Result, error) { return Result{}, nil }}
	es := NewEventStore(&dispatcher, 1<<16, DropOldest)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = es.Subscribe(bg, Event{ID: "pp", Projection: "evt", Args: nil})
		}
	})
}

func BenchmarkPublish(b *testing.B) {
	dispatcher := Dispatcher{"evt": func(_ context.Context, args map[string]any) (Result, error) { return Result{}, nil }}
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
	dispatcher := Dispatcher{"evt": func(_ context.Context, args map[string]any) (Result, error) { return Result{}, nil }}
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
	dispatcher := Dispatcher{"evtLarge": func(_ context.Context, args map[string]any) (Result, error) { return Result{}, nil }}
	es := NewEventStore(&dispatcher, 1<<16, DropOldest)
	var payload LargeStruct

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = es.Subscribe(bg, Event{ID: "bench", Projection: "evtLarge", Args: map[string]any{"payload": payload}})
	}
}

func BenchmarkPublishLargePayload(b *testing.B) {
	dispatcher := Dispatcher{"evtLarge": func(_ context.Context, args map[string]any) (Result, error) { return Result{}, nil }}
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
	dispatcher["evtExact"] = func(_ context.Context, args map[string]any) (Result, error) {
		atomic.AddUint64(&count, 1)
		return Result{}, nil
	}
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
	dispatcher["evtThresh"] = func(_ context.Context, args map[string]any) (Result, error) {
		atomic.AddUint64(&count, 1)
		return Result{}, nil
	}
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
	dispatcher["evtOnce"] = func(_ context.Context, args map[string]any) (Result, error) {
		atomic.AddInt32(&called, 1)
		return Result{}, nil
	}
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
		"print": func(_ context.Context, args map[string]any) (Result, error) {
			mu.Lock()
			defer mu.Unlock()
			called++
			return Result{Message: "ok"}, nil
		},
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

func BenchmarkEventStore_Async(b *testing.B) {
	dispatcher := Dispatcher{"async": func(_ context.Context, args map[string]any) (Result, error) { return Result{Message: "done"}, nil }}
	store := NewEventStore(&dispatcher, 1<<16, DropOldest)
	store.Async = true
	for i := 0; i < b.N; i++ {
		_ = store.Subscribe(bg, Event{ID: "event", Projection: "async", Args: map[string]any{"n": i}})
	}
	store.Publish()
}

func BenchmarkEventStore_Sync(b *testing.B) {
	dispatcher := Dispatcher{"sync": func(_ context.Context, args map[string]any) (Result, error) { return Result{Message: "done"}, nil }}
	store := NewEventStore(&dispatcher, 1<<16, DropOldest)
	store.Async = false
	for i := 0; i < b.N; i++ {
		_ = store.Subscribe(bg, Event{ID: "event", Projection: "sync", Args: map[string]any{"n": i}})
	}
	store.Publish()
}

// FastHTTP benchmarks updated for new API
func benchmarkFastHTTP(b *testing.B, async bool) {
	dispatcher := Dispatcher{"evt": func(_ context.Context, args map[string]any) (Result, error) { return Result{}, nil }}
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
	dispatcher := Dispatcher{"evt": func(_ context.Context, args map[string]any) (Result, error) { return Result{}, nil }}
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
	initialHead, initialTail := es.head, es.tail
	es.Publish()
	if es.head != initialHead || es.tail != initialTail {
		t.Errorf("counters changed: head %d->%d tail %d->%d", initialHead, es.head, initialTail, es.tail)
	}
}

// TestArgsPassing ensures arguments are passed through
func TestArgsPassing(t *testing.T) {
	dispatcher := Dispatcher{}
	var received string
	dispatcher["echo"] = func(_ context.Context, args map[string]any) (Result, error) {
		received = args["foo"].(string)
		return Result{}, nil
	}
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
	dispatcher["snap"] = func(_ context.Context, args map[string]any) (Result, error) {
		atomic.AddInt32(&calledOriginal, 1)
		return Result{}, nil
	}
	es := NewEventStore(&dispatcher, 1<<16, DropOldest)
	// swap handler
	dispatcher["snap"] = func(_ context.Context, args map[string]any) (Result, error) {
		atomic.AddInt32(&calledModified, 1)
		return Result{}, nil
	}
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
	dispatcher := Dispatcher{"metric": func(_ context.Context, args map[string]any) (Result, error) { return Result{}, nil }}
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
func noopHandler(counter *uint64) func(context.Context, map[string]any) (Result, error) {
	return func(ctx context.Context, args map[string]any) (Result, error) {
		atomic.AddUint64(counter, 1)
		return Result{Message: "ok"}, nil
	}
}

// TestMetricsCounts ensures that the published/processed/error counters are accurate.
func TestMetricsCounts(t *testing.T) {
	var invoked uint64
	disp := Dispatcher{"test": noopHandler(&invoked)}
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
		disp := Dispatcher{"bench": noopHandler(&invoked)}
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

// TestContextPropagation verifies that a context injected through Args["__ctx"]
// is forwarded to the handler.
func TestContextPropagation(t *testing.T) {
	const key, val = "myKey", "myVal"

	// prepare a context with a distinctive value
	ctx := context.WithValue(context.Background(), key, val)

	var received string
	disp := Dispatcher{
		"ctx": func(c context.Context, _ map[string]any) (Result, error) {
			if v, ok := c.Value(key).(string); ok {
				received = v
			}
			return Result{}, nil
		},
	}

	es := NewEventStore(&disp, 8, DropOldest)
	// inject ctx via the reserved "__ctx" argument key
	_ = es.Subscribe(context.Background(), Event{ID: "1", Projection: "ctx", Args: map[string]any{"__ctx": ctx}})
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
	disp := Dispatcher{"boom": func(_ context.Context, _ map[string]any) (Result, error) {
		return Result{}, errors.New("boom")
	}}
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
	disp := Dispatcher{"err": func(_ context.Context, _ map[string]any) (Result, error) { return Result{}, errors.New("fail") }}
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
	disp["echo"] = func(ctx context.Context, args map[string]any) (Result, error) {
		return Result{Message: args["msg"].(string)}, nil
	}
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
	disp["inc"] = func(ctx context.Context, args map[string]any) (Result, error) {
		// return current value
		return Result{Message: ""}, nil
	}
	store := NewEventStore(&disp, 8, DropOldest)
	// middleware increments counter before handler
	store.Use(func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, args map[string]any) (Result, error) {
			args["cnt"] = args["cnt"].(int) + 1
			return next(ctx, args)
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
	disp["fail"] = func(ctx context.Context, args map[string]any) (Result, error) {
		return Result{}, errors.New(errorMsg)
	}
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
		"testEvent": func(ctx context.Context, args map[string]any) (Result, error) {
			processed.Add(1)
			return Result{Message: "ok"}, nil
		},
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
func noOpHandler(ctx context.Context, args map[string]any) (Result, error) {
	return Result{}, nil
}

// setupEventStore initializes an EventStore with the given async flag and buffer size.
func setupEventStore(async bool, bufferSize uint64) *EventStore {
	disp := Dispatcher{"test": noOpHandler}
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
		"foo": func(ctx context.Context, args map[string]any) (Result, error) {
			atomic.StoreUint32(&called, 1)
			return Result{}, nil
		},
	}
	es := NewEventStore(&dispatcher, 8, DropOldest)
	// schedule 50ms in future
	es.ScheduleAfter(context.Background(), 50*time.Millisecond, Event{Projection: "foo"})
	time.Sleep(100 * time.Millisecond)
	if atomic.LoadUint32(&called) != 1 {
		t.Fatal("expected handler to fire once")
	}
}
