package GoEventBus

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

// TestBatch_Basic verifies that a batch handler receives all events published for its projection.
func TestBatch_Basic(t *testing.T) {
	disp := Dispatcher{}
	es := NewEventStore(&disp, 1<<16, DropOldest)

	var got []Event
	var mu sync.Mutex
	es.RegisterBatch("order.placed", 100, func(_ context.Context, evs []Event) ([]Result, error) {
		mu.Lock()
		got = append(got, evs...)
		mu.Unlock()
		return nil, nil
	})

	for i := 0; i < 5; i++ {
		_ = es.Subscribe(bg, Event{Projection: "order.placed"})
	}
	es.Publish()

	mu.Lock()
	defer mu.Unlock()
	if len(got) != 5 {
		t.Fatalf("batch handler received %d events; want 5", len(got))
	}
}

// TestBatch_Chunking verifies that events are chunked by the configured size.
func TestBatch_Chunking(t *testing.T) {
	disp := Dispatcher{}
	es := NewEventStore(&disp, 1<<16, DropOldest)

	var calls []int // lengths of each call
	var mu sync.Mutex
	es.RegisterBatch("tick", 3, func(_ context.Context, evs []Event) ([]Result, error) {
		mu.Lock()
		calls = append(calls, len(evs))
		mu.Unlock()
		return nil, nil
	})

	for i := 0; i < 7; i++ {
		_ = es.Subscribe(bg, Event{Projection: "tick"})
	}
	es.Publish()

	mu.Lock()
	defer mu.Unlock()
	// 7 events with size=3 → chunks of [3, 3, 1]
	if len(calls) != 3 {
		t.Fatalf("handler called %d times; want 3", len(calls))
	}
	if calls[0] != 3 || calls[1] != 3 || calls[2] != 1 {
		t.Fatalf("chunk sizes %v; want [3 3 1]", calls)
	}
}

// TestBatch_FanOut verifies that multiple batch handlers on the same projection
// each receive all events independently.
func TestBatch_FanOut(t *testing.T) {
	disp := Dispatcher{}
	es := NewEventStore(&disp, 1<<16, DropOldest)

	var countA, countB int32
	es.RegisterBatch("evt", 100, func(_ context.Context, evs []Event) ([]Result, error) {
		atomic.AddInt32(&countA, int32(len(evs)))
		return nil, nil
	})
	es.RegisterBatch("evt", 100, func(_ context.Context, evs []Event) ([]Result, error) {
		atomic.AddInt32(&countB, int32(len(evs)))
		return nil, nil
	})

	for i := 0; i < 4; i++ {
		_ = es.Subscribe(bg, Event{Projection: "evt"})
	}
	es.Publish()

	if countA != 4 {
		t.Errorf("handler A received %d events; want 4", countA)
	}
	if countB != 4 {
		t.Errorf("handler B received %d events; want 4", countB)
	}
}

// TestBatch_MixedWithRegular verifies that batch and regular handlers for the
// same projection both fire independently.
func TestBatch_MixedWithRegular(t *testing.T) {
	disp := Dispatcher{}
	var regularCalls int32
	disp.Register("mixed", func(_ context.Context, ev Event) (Result, error) {
		atomic.AddInt32(&regularCalls, 1)
		return Result{}, nil
	})

	es := NewEventStore(&disp, 1<<16, DropOldest)
	var batchTotal int32
	es.RegisterBatch("mixed", 100, func(_ context.Context, evs []Event) ([]Result, error) {
		atomic.AddInt32(&batchTotal, int32(len(evs)))
		return nil, nil
	})

	for i := 0; i < 3; i++ {
		_ = es.Subscribe(bg, Event{Projection: "mixed"})
	}
	es.Publish()

	if regularCalls != 3 {
		t.Errorf("regular handler called %d times; want 3", regularCalls)
	}
	if batchTotal != 3 {
		t.Errorf("batch handler received %d events; want 3", batchTotal)
	}
}

// TestBatch_DLQ verifies that a batch handler error sends all events in the
// failing chunk to the dead-letter queue.
func TestBatch_DLQ(t *testing.T) {
	disp := Dispatcher{}
	es := NewEventStore(&disp, 1<<16, DropOldest)
	es.DLQ = NewDeadLetterQueue()

	batchErr := errors.New("bulk insert failed")
	es.RegisterBatch("write", 100, func(_ context.Context, _ []Event) ([]Result, error) {
		return nil, batchErr
	})

	for i := 0; i < 4; i++ {
		_ = es.Subscribe(bg, Event{Projection: "write"})
	}
	es.Publish()

	entries := es.DLQ.Entries()
	if len(entries) != 4 {
		t.Fatalf("DLQ has %d entries; want 4", len(entries))
	}
	for _, dl := range entries {
		if !errors.Is(dl.Err, batchErr) {
			t.Errorf("DLQ entry error = %v; want %v", dl.Err, batchErr)
		}
		if dl.Attempts != 1 {
			t.Errorf("DLQ entry Attempts = %d; want 1", dl.Attempts)
		}
	}
}

// TestBatch_DLQ_PerChunk verifies that only the failing chunk lands in DLQ
// when events span multiple chunks and one chunk succeeds.
func TestBatch_DLQ_PerChunk(t *testing.T) {
	disp := Dispatcher{}
	es := NewEventStore(&disp, 1<<16, DropOldest)
	es.DLQ = NewDeadLetterQueue()

	var calls int32
	batchErr := errors.New("second chunk failed")
	es.RegisterBatch("write", 2, func(_ context.Context, evs []Event) ([]Result, error) {
		n := atomic.AddInt32(&calls, 1)
		if n == 2 {
			return nil, batchErr
		}
		return nil, nil
	})

	for i := 0; i < 4; i++ {
		_ = es.Subscribe(bg, Event{Projection: "write"})
	}
	es.Publish()

	// 4 events, size=2 → 2 chunks. First succeeds, second fails → 2 in DLQ.
	entries := es.DLQ.Entries()
	if len(entries) != 2 {
		t.Fatalf("DLQ has %d entries; want 2", len(entries))
	}
}

// TestBatch_PanicRecovery verifies that a panicking batch handler is recovered
// and all events in the chunk land in the DLQ.
func TestBatch_PanicRecovery(t *testing.T) {
	disp := Dispatcher{}
	es := NewEventStore(&disp, 1<<16, DropOldest)
	es.DLQ = NewDeadLetterQueue()

	es.RegisterBatch("panic", 100, func(_ context.Context, _ []Event) ([]Result, error) {
		panic("batch exploded")
	})

	for i := 0; i < 3; i++ {
		_ = es.Subscribe(bg, Event{Projection: "panic"})
	}
	es.Publish() // must not panic

	if es.DLQ.Len() != 3 {
		t.Errorf("DLQ has %d entries; want 3", es.DLQ.Len())
	}
}

// TestBatch_OnErrorHook verifies that OnError fires once per event in a
// failing batch chunk.
func TestBatch_OnErrorHook(t *testing.T) {
	disp := Dispatcher{}
	es := NewEventStore(&disp, 1<<16, DropOldest)

	var errorFires int32
	es.OnError(func(_ context.Context, _ Event, _ error) {
		atomic.AddInt32(&errorFires, 1)
	})

	es.RegisterBatch("failing", 100, func(_ context.Context, _ []Event) ([]Result, error) {
		return nil, errors.New("fail")
	})

	for i := 0; i < 5; i++ {
		_ = es.Subscribe(bg, Event{Projection: "failing"})
	}
	es.Publish()

	if errorFires != 5 {
		t.Errorf("OnError fired %d times; want 5", errorFires)
	}
}

// TestBatch_Metrics verifies that processedCount is incremented per event, not
// per batch call.
func TestBatch_Metrics(t *testing.T) {
	disp := Dispatcher{}
	es := NewEventStore(&disp, 1<<16, DropOldest)
	es.RegisterBatch("m", 100, func(_ context.Context, _ []Event) ([]Result, error) {
		return nil, nil
	})

	for i := 0; i < 6; i++ {
		_ = es.Subscribe(bg, Event{Projection: "m"})
	}
	es.Publish()

	_, processed, _ := es.Metrics()
	if processed != 6 {
		t.Errorf("processed = %d; want 6", processed)
	}
}

// TestBatch_Async verifies batch handlers work correctly in async mode.
func TestBatch_Async(t *testing.T) {
	disp := Dispatcher{}
	es := NewEventStore(&disp, 1<<16, DropOldest)
	es.Async = true

	var received int32
	es.RegisterBatch("async", 3, func(_ context.Context, evs []Event) ([]Result, error) {
		atomic.AddInt32(&received, int32(len(evs)))
		return nil, nil
	})

	for i := 0; i < 9; i++ {
		_ = es.Subscribe(bg, Event{Projection: "async"})
	}
	es.Publish()
	_ = es.Drain(context.Background())

	if received != 9 {
		t.Errorf("async batch received %d events; want 9", received)
	}
}

// TestBatch_Results verifies that per-event Results returned by the handler are
// forwarded to OnAfter hooks.
func TestBatch_Results(t *testing.T) {
	disp := Dispatcher{}
	es := NewEventStore(&disp, 1<<16, DropOldest)

	es.RegisterBatch("res", 100, func(_ context.Context, evs []Event) ([]Result, error) {
		out := make([]Result, len(evs))
		for i := range out {
			out[i] = Result{Message: "ok"}
		}
		return out, nil
	})

	var afterMessages []string
	var mu sync.Mutex
	es.OnAfter(func(_ context.Context, _ Event, r Result, _ error) {
		mu.Lock()
		afterMessages = append(afterMessages, r.Message)
		mu.Unlock()
	})

	for i := 0; i < 3; i++ {
		_ = es.Subscribe(bg, Event{Projection: "res"})
	}
	es.Publish()

	mu.Lock()
	defer mu.Unlock()
	if len(afterMessages) != 3 {
		t.Fatalf("OnAfter fired %d times; want 3", len(afterMessages))
	}
	for _, msg := range afterMessages {
		if msg != "ok" {
			t.Errorf("OnAfter result.Message = %q; want \"ok\"", msg)
		}
	}
}
