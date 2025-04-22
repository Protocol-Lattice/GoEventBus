package GoEventBus

import (
	"sync/atomic"
	"testing"
)

// TestSubscribeAndPublish verifies that events are stored and published correctly.
func TestSubscribeAndPublish(t *testing.T) {
	// Prepare dispatcher with two handlers
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

	// Subscribe two events
	es.Subscribe(Event{ID: "1", Projection: "evt1", Args: nil})
	es.Subscribe(Event{ID: "2", Projection: "evt2", Args: nil})

	// Publish all events
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
	// Dispatcher without handlers
	dispatcher := Dispatcher{}
	es := NewEventStore(&dispatcher)

	// Subscribe an event with no corresponding handler
	es.Subscribe(Event{ID: "3", Projection: "unknown", Args: nil})

	// Should not panic
	es.Publish()
}

// BenchmarkSubscribe measures the performance of Subscribe under load.
func BenchmarkSubscribe(b *testing.B) {
	dispatcher := Dispatcher{"evt": func(args map[string]any) (Result, error) { return Result{}, nil }}
	es := NewEventStore(&dispatcher)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		es.Subscribe(Event{ID: "bench", Projection: "evt", Args: nil})
	}
}

// BenchmarkPublish measures the performance of Publish over a populated buffer.
func BenchmarkPublish(b *testing.B) {
	dispatcher := Dispatcher{"evt": func(args map[string]any) (Result, error) { return Result{}, nil }}
	es := NewEventStore(&dispatcher)

	// Pre-fill buffer with events
	for i := 0; i < size; i++ {
		es.Subscribe(Event{ID: "bench", Projection: "evt", Args: nil})
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

// BenchmarkSubscribeLargePayload benchmarks Subscribe with large struct payloads.
func BenchmarkSubscribeLargePayload(b *testing.B) {
	dispatcher := Dispatcher{"evtLarge": func(args map[string]any) (Result, error) { return Result{}, nil }}
	es := NewEventStore(&dispatcher)
	var payload LargeStruct

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		es.Subscribe(Event{ID: "bench", Projection: "evtLarge", Args: map[string]any{"payload": payload}})
	}
}

// BenchmarkPublishLargePayload benchmarks Publish on a buffer pre-filled with events carrying large payloads.
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
