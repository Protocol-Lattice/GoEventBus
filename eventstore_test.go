package GoEventBus

import (
	"context"
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
