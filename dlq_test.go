package GoEventBus

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestDLQ_HandlerError verifies that a handler returning an error routes the
// event to the DLQ with Attempts==1 and the correct error.
func TestDLQ_HandlerError(t *testing.T) {
	sentinel := errors.New("boom")
	disp := Dispatcher{}
	disp.Register("evt", func(_ context.Context, ev Event) (Result, error) {
		return Result{}, sentinel
	})

	es := NewEventStore(&disp, 8, DropOldest)
	es.DLQ = NewDeadLetterQueue()

	_ = es.Subscribe(context.Background(), Event{ID: "e1", Projection: "evt"})
	es.Publish()

	if es.DLQ.Len() != 1 {
		t.Fatalf("expected 1 dead letter; got %d", es.DLQ.Len())
	}
	dl := es.DLQ.Entries()[0]
	if dl.Event.ID != "e1" {
		t.Errorf("wrong event ID: %q", dl.Event.ID)
	}
	if !errors.Is(dl.Err, sentinel) {
		t.Errorf("expected sentinel error; got %v", dl.Err)
	}
	if dl.Attempts != 1 {
		t.Errorf("expected Attempts==1; got %d", dl.Attempts)
	}
	if dl.FailedAt.IsZero() {
		t.Error("FailedAt should not be zero")
	}
}

// TestDLQ_HandlerPanic verifies that a panicking handler routes the event to
// the DLQ with a wrapped panic error and does not crash the process.
func TestDLQ_HandlerPanic(t *testing.T) {
	disp := Dispatcher{}
	disp.Register("evt", func(_ context.Context, ev Event) (Result, error) {
		panic("something exploded")
	})

	es := NewEventStore(&disp, 8, DropOldest)
	es.DLQ = NewDeadLetterQueue()

	_ = es.Subscribe(context.Background(), Event{ID: "p1", Projection: "evt"})
	es.Publish() // must not panic

	if es.DLQ.Len() != 1 {
		t.Fatalf("expected 1 dead letter; got %d", es.DLQ.Len())
	}
	dl := es.DLQ.Entries()[0]
	if !strings.Contains(dl.Err.Error(), "handler panic") {
		t.Errorf("expected panic wrapper in error; got %v", dl.Err)
	}
	if !strings.Contains(dl.Err.Error(), "something exploded") {
		t.Errorf("expected original panic message in error; got %v", dl.Err)
	}
}

// TestDLQ_PanicWrapsError verifies that a panic(err) (not a string) is wrapped
// with errors.Is semantics.
func TestDLQ_PanicWrapsError(t *testing.T) {
	sentinel := errors.New("inner error")
	disp := Dispatcher{}
	disp.Register("evt", func(_ context.Context, ev Event) (Result, error) {
		panic(sentinel)
	})

	es := NewEventStore(&disp, 8, DropOldest)
	es.DLQ = NewDeadLetterQueue()

	_ = es.Subscribe(context.Background(), Event{Projection: "evt"})
	es.Publish()

	dl := es.DLQ.Entries()[0]
	if !errors.Is(dl.Err, sentinel) {
		t.Errorf("expected errors.Is to find sentinel; got %v", dl.Err)
	}
}

// TestDLQ_NilDLQ verifies that behaviour is unchanged when no DLQ is attached.
func TestDLQ_NilDLQ(t *testing.T) {
	sentinel := errors.New("fail")
	disp := Dispatcher{}
	disp.Register("evt", func(_ context.Context, ev Event) (Result, error) {
		return Result{}, sentinel
	})

	es := NewEventStore(&disp, 8, DropOldest)
	// DLQ intentionally not set

	_ = es.Subscribe(context.Background(), Event{Projection: "evt"})
	es.Publish() // must not panic

	_, _, errs := es.Metrics()
	if errs != 1 {
		t.Errorf("expected errorCount==1; got %d", errs)
	}
}

// TestDLQ_Entries_IsCopy verifies that Entries returns a copy so mutations
// to the returned slice do not affect the queue.
func TestDLQ_Entries_IsCopy(t *testing.T) {
	q := NewDeadLetterQueue()
	q.add(DeadLetter{Event: Event{ID: "x"}, Attempts: 1})

	snap := q.Entries()
	snap[0].Attempts = 99

	if q.Entries()[0].Attempts == 99 {
		t.Error("Entries() returned a reference, not a copy")
	}
}

// TestDLQ_Drain empties the queue and returns all entries.
func TestDLQ_Drain(t *testing.T) {
	q := NewDeadLetterQueue()
	q.add(DeadLetter{Event: Event{ID: "a"}, Attempts: 1})
	q.add(DeadLetter{Event: Event{ID: "b"}, Attempts: 1})

	out := q.Drain()
	if len(out) != 2 {
		t.Fatalf("expected 2 entries; got %d", len(out))
	}
	if q.Len() != 0 {
		t.Errorf("queue should be empty after Drain; len=%d", q.Len())
	}
}

// TestDLQ_Replay re-enqueues dead letters and verifies they are reprocessed.
func TestDLQ_Replay(t *testing.T) {
	var calls atomic.Int32
	disp := Dispatcher{}
	disp.Register("evt", func(_ context.Context, ev Event) (Result, error) {
		calls.Add(1)
		return Result{}, nil
	})

	es := NewEventStore(&disp, 16, DropOldest)
	es.DLQ = NewDeadLetterQueue()

	// Manually seed the DLQ with two entries.
	es.DLQ.add(DeadLetter{Event: Event{ID: "d1", Projection: "evt"}, Attempts: 1})
	es.DLQ.add(DeadLetter{Event: Event{ID: "d2", Projection: "evt"}, Attempts: 1})

	if err := es.DLQ.Replay(context.Background(), es); err != nil {
		t.Fatalf("Replay returned error: %v", err)
	}

	// Replay calls Publish internally.
	if es.DLQ.Len() != 0 {
		t.Errorf("expected empty DLQ after successful replay; len=%d", es.DLQ.Len())
	}
	if calls.Load() != 2 {
		t.Errorf("expected 2 handler calls; got %d", calls.Load())
	}
}

// TestDLQ_Replay_IncrementsAttempts verifies that Replay increments Attempts
// on entries that fail to re-subscribe and keeps them in the queue.
func TestDLQ_Replay_IncrementsAttempts(t *testing.T) {
	disp := Dispatcher{}
	// Buffer of 1 — pre-fill it so Subscribe always returns ErrBufferFull.
	es := NewEventStore(&disp, 1, ReturnError)
	es.DLQ = NewDeadLetterQueue()
	// Pre-fill the single slot.
	_ = es.Subscribe(context.Background(), Event{Projection: "x"})

	es.DLQ.add(DeadLetter{Event: Event{ID: "stuck", Projection: "x"}, Attempts: 1})

	err := es.DLQ.Replay(context.Background(), es)
	if !errors.Is(err, ErrBufferFull) {
		t.Fatalf("expected ErrBufferFull; got %v", err)
	}
	if es.DLQ.Len() != 1 {
		t.Fatalf("failed entry should remain in DLQ; len=%d", es.DLQ.Len())
	}
	if es.DLQ.Entries()[0].Attempts != 2 {
		t.Errorf("expected Attempts==2 after failed replay; got %d", es.DLQ.Entries()[0].Attempts)
	}
}

// TestDLQ_Async verifies DLQ routing works in async mode.
func TestDLQ_Async(t *testing.T) {
	sentinel := errors.New("async fail")
	disp := Dispatcher{}
	disp.Register("evt", func(_ context.Context, ev Event) (Result, error) {
		return Result{}, sentinel
	})

	es := NewEventStore(&disp, 16, DropOldest)
	es.DLQ = NewDeadLetterQueue()
	es.Async = true

	for i := 0; i < 5; i++ {
		_ = es.Subscribe(context.Background(), Event{Projection: "evt"})
	}
	es.Publish()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := es.Drain(ctx); err != nil {
		t.Fatalf("Drain: %v", err)
	}

	if es.DLQ.Len() != 5 {
		t.Errorf("expected 5 dead letters; got %d", es.DLQ.Len())
	}
	for _, dl := range es.DLQ.Entries() {
		if !errors.Is(dl.Err, sentinel) {
			t.Errorf("unexpected error: %v", dl.Err)
		}
	}
}

// TestDLQ_FanOut_PartialFailure verifies that in a fan-out, only the failing
// handler's event is dead-lettered; the successful handler is not.
func TestDLQ_FanOut_PartialFailure(t *testing.T) {
	sentinel := errors.New("partial fail")
	var goodCalled atomic.Int32
	disp := Dispatcher{}
	disp.Register("evt",
		func(_ context.Context, ev Event) (Result, error) { return Result{}, sentinel },
		func(_ context.Context, ev Event) (Result, error) { goodCalled.Add(1); return Result{}, nil },
	)

	es := NewEventStore(&disp, 8, DropOldest)
	es.DLQ = NewDeadLetterQueue()

	_ = es.Subscribe(context.Background(), Event{ID: "fanout", Projection: "evt"})
	es.Publish()

	if es.DLQ.Len() != 1 {
		t.Errorf("expected 1 dead letter (only the failing handler); got %d", es.DLQ.Len())
	}
	if goodCalled.Load() != 1 {
		t.Errorf("expected successful handler to run once; got %d", goodCalled.Load())
	}
}
