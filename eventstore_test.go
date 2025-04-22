package GoEventBus

import (
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
)

// TestSubscribeAndPublish verifies that events are stored and published correctly.
func TestSubscribeAndPublish(t *testing.T) {
	dispatcher := Dispatcher{}
	var called1, called2 int32
	dispatcher["evt1"] = func(args map[string]any) (Result, error) {
		atomic.AddInt32(&called1, 1)
		return Result{Message: "ok1"}, nil
	}
	dispatcher["evt2"] = func(args map[string]any) (Result, error) {
		atomic.AddInt32(&called2, 1)
		return Result{Message: "ok2"}, nil
	}

	es := NewEventStore(&dispatcher)

	es.Subscribe(Event{ID: "1", Projection: "evt1", Args: nil})
	es.Subscribe(Event{ID: "2", Projection: "evt2", Args: nil})

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
	es := NewEventStore(&dispatcher)

	// Subscribe an event with no matching handler
	es.Subscribe(Event{ID: "3", Projection: "unknown", Args: nil})

	// Should not panic
	es.Publish()
}

// TestMissingAndExistingProjections ensures missing projections don't affect existing handlers.
func TestPublishMixedExistingAndNonExisting(t *testing.T) {
	dispatcher := Dispatcher{}
	var called int32
	dispatcher["evt"] = func(args map[string]any) (Result, error) {
		atomic.AddInt32(&called, 1)
		return Result{}, nil
	}
	es := NewEventStore(&dispatcher)

	// Subscribe one existing and one non-existing projection
	es.Subscribe(Event{ID: "1", Projection: "evt", Args: nil})
	es.Subscribe(Event{ID: "2", Projection: "noexist", Args: nil})

	es.Publish()

	if called != 1 {
		t.Errorf("handler called %d times; want 1", called)
	}
}

// TestOverflowBehavior ensures that when more events than buffer size are enqueued,
// the oldest events are dropped.
func TestOverflowBehavior(t *testing.T) {
	dispatcher := Dispatcher{}
	var count uint64
	dispatcher["evtOverflow"] = func(args map[string]any) (Result, error) {
		atomic.AddUint64(&count, 1)
		return Result{}, nil
	}
	es := NewEventStore(&dispatcher)

	// Enqueue size + 100 events to overflow
	for i := 0; i < size+100; i++ {
		es.Subscribe(Event{ID: "o", Projection: "evtOverflow", Args: nil})
	}
	es.Publish()

	if count != size {
		t.Errorf("overflow: got %d events; want %d", count, size)
	}
}

// TestConcurrentSubscribe ensures safety of concurrent subscriptions.
func TestConcurrentSubscribe(t *testing.T) {
	dispatcher := Dispatcher{"evt": func(args map[string]any) (Result, error) { return Result{}, nil }}
	var count uint64
	dispatcher["evt"] = func(args map[string]any) (Result, error) {
		atomic.AddUint64(&count, 1)
		return Result{}, nil
	}
	es := NewEventStore(&dispatcher)

	var wg sync.WaitGroup
	const n = 1000
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			es.Subscribe(Event{ID: "c", Projection: "evt", Args: nil})
			wg.Done()
		}()
	}
	wg.Wait()
	es.Publish()

	if count != n {
		t.Errorf("concurrent subscribe: got %d events; want %d", count, n)
	}
}

// Benchmarks
func BenchmarkSubscribe(b *testing.B) {
	dispatcher := Dispatcher{"evt": func(args map[string]any) (Result, error) { return Result{}, nil }}
	es := NewEventStore(&dispatcher)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		es.Subscribe(Event{ID: "bench", Projection: "evt", Args: nil})
	}
}

func BenchmarkSubscribeParallel(b *testing.B) {
	dispatcher := Dispatcher{"evt": func(args map[string]any) (Result, error) { return Result{}, nil }}
	es := NewEventStore(&dispatcher)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			es.Subscribe(Event{ID: "pp", Projection: "evt", Args: nil})
		}
	})
}

func BenchmarkPublish(b *testing.B) {
	dispatcher := Dispatcher{"evt": func(args map[string]any) (Result, error) { return Result{}, nil }}
	es := NewEventStore(&dispatcher)

	// Pre-fill buffer with events
	for i := 0; i < size; i++ {
		es.Subscribe(Event{ID: "p", Projection: "evt", Args: nil})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		es.Publish()
	}
}

func BenchmarkPublishAfterPrefill(b *testing.B) {
	dispatcher := Dispatcher{"evt": func(args map[string]any) (Result, error) { return Result{}, nil }}
	es := NewEventStore(&dispatcher)

	// Pre-fill buffer with events
	for i := 0; i < size; i++ {
		es.Subscribe(Event{ID: "pp", Projection: "evt", Args: nil})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		es.Publish()
	}
}

// LargeStruct is a sample struct for payload-heavy benchmarks.
type LargeStruct struct {
	Data [1024]byte
}

func BenchmarkSubscribeLargePayload(b *testing.B) {
	dispatcher := Dispatcher{"evtLarge": func(args map[string]any) (Result, error) { return Result{}, nil }}
	es := NewEventStore(&dispatcher)
	var payload LargeStruct

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		es.Subscribe(Event{ID: "bench", Projection: "evtLarge", Args: map[string]any{"payload": payload}})
	}
}

func BenchmarkPublishLargePayload(b *testing.B) {
	dispatcher := Dispatcher{"evtLarge": func(args map[string]any) (Result, error) { return Result{}, nil }}
	es := NewEventStore(&dispatcher)
	var payload LargeStruct

	// Pre-fill buffer
	const largeSize = 100
	for i := 0; i < largeSize; i++ {
		es.Subscribe(Event{ID: "bench", Projection: "evtLarge", Args: map[string]any{"payload": payload}})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		es.Publish()
	}
}

// TestExactBufferSizeNoOverflow enqueues exactly size events (no overflow),
// and ensures Publish calls the handler size times.
func TestExactBufferSizeNoOverflow(t *testing.T) {
	dispatcher := Dispatcher{}
	var count uint64
	dispatcher["evtExact"] = func(args map[string]any) (Result, error) {
		atomic.AddUint64(&count, 1)
		return Result{}, nil
	}
	es := NewEventStore(&dispatcher)

	for i := 0; i < size; i++ {
		es.Subscribe(Event{ID: strconv.Itoa(i), Projection: "evtExact", Args: nil})
	}
	es.Publish()

	if count != size {
		t.Errorf("no-overflow: got %d calls; want %d", count, size)
	}
}

// TestOverflowThreshold enqueues size+1 events to trigger the exact threshold
// overflow (idx-tail == size), dropping the first event.
func TestOverflowThreshold(t *testing.T) {
	dispatcher := Dispatcher{}
	var count uint64
	dispatcher["evtThresh"] = func(args map[string]any) (Result, error) {
		atomic.AddUint64(&count, 1)
		return Result{}, nil
	}
	es := NewEventStore(&dispatcher)

	for i := 0; i < size+1; i++ {
		es.Subscribe(Event{ID: strconv.Itoa(i), Projection: "evtThresh", Args: nil})
	}
	es.Publish()

	if count != size {
		t.Errorf("threshold-overflow: got %d calls; want %d", count, size)
	}
}

// TestPublishIdempotent ensures that calling Publish twice without new events
// only invokes handlers once (early-return path when tail == head).
func TestPublishIdempotent(t *testing.T) {
	dispatcher := Dispatcher{}
	var called int32
	dispatcher["evtOnce"] = func(args map[string]any) (Result, error) {
		atomic.AddInt32(&called, 1)
		return Result{}, nil
	}
	es := NewEventStore(&dispatcher)

	es.Subscribe(Event{ID: "1", Projection: "evtOnce", Args: nil})
	es.Publish()
	es.Publish() // should return immediately, not call handler again

	if called != 1 {
		t.Errorf("idempotent publish: got %d calls; want 1", called)
	}
}
