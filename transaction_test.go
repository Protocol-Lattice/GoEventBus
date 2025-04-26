package GoEventBus

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

// dummy handler just increments a counter
func makeCounterHandler(counter *uint64) HandlerFunc {
	return func(ctx context.Context, args map[string]any) (Result, error) {
		atomic.AddUint64(counter, 1)
		return Result{Message: "ok"}, nil
	}
}

func TestTransaction_CommitAndRollback(t *testing.T) {
	// set up dispatcher with a no-op projection
	var processed uint64
	dispatcher := Dispatcher{
		"p": makeCounterHandler(&processed),
	}

	es := NewEventStore(&dispatcher, 8, DropOldest)
	ctx := context.Background()

	// Test Commit
	tx := es.BeginTransaction()
	tx.Publish(Event{ID: "1", Projection: "p", Args: map[string]any{}})
	tx.Publish(Event{ID: "2", Projection: "p", Args: map[string]any{}})
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
	// actually publish into store
	es.Publish()
	if got := atomic.LoadUint64(&processed); got != 2 {
		t.Errorf("expected 2 events processed, got %d", got)
	}

	// Test Rollback
	processed = 0
	tx2 := es.BeginTransaction()
	tx2.Publish(Event{ID: "3", Projection: "p", Args: map[string]any{}})
	tx2.Rollback()
	if err := tx2.Commit(ctx); err != nil {
		t.Fatalf("Commit after rollback should not error, got %v", err)
	}
	es.Publish()
	if got := atomic.LoadUint64(&processed); got != 0 {
		t.Errorf("expected 0 events after rollback, got %d", got)
	}
}

func TestTransaction_PartialFailure(t *testing.T) {
	// handler that errors on the second event
	cnt := uint64(0)
	dispatcher := Dispatcher{
		"x": func(ctx context.Context, args map[string]any) (Result, error) {
			i := atomic.AddUint64(&cnt, 1)
			if i == 2 {
				return Result{}, errors.New("boom")
			}
			return Result{Message: "ok"}, nil
		},
	}

	es := NewEventStore(&dispatcher, 4, ReturnError)
	ctx := context.Background()

	tx := es.BeginTransaction()
	tx.Publish(Event{ID: "a", Projection: "x", Args: map[string]any{}})
	tx.Publish(Event{ID: "b", Projection: "x", Args: map[string]any{}})
	err := tx.Commit(ctx)
	if err == nil {
		t.Fatal("expected Commit to return error on second event, got nil")
	}
	if err.Error() != "goeventbus: buffer is full" && err.Error() != "boom" {
		// Depending on overrun policy, could bubble Subscribe error or handler error
		t.Fatalf("unexpected error: %v", err)
	}
}

func BenchmarkTransaction_SyncCommit(b *testing.B) {
	dispatcher := Dispatcher{
		"p": func(ctx context.Context, args map[string]any) (Result, error) {
			return Result{Message: "ok"}, nil
		},
	}
	es := NewEventStore(&dispatcher, 256, DropOldest)
	es.Async = false

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx := es.BeginTransaction()
		// buffer N events per transaction
		for j := 0; j < 16; j++ {
			tx.Publish(Event{ID: "t", Projection: "p", Args: map[string]any{}})
		}
		if err := tx.Commit(context.Background()); err != nil {
			b.Fatalf("Commit error: %v", err)
		}
		es.Publish()
	}
}

func BenchmarkTransaction_AsyncCommit(b *testing.B) {
	dispatcher := Dispatcher{
		"p": func(ctx context.Context, args map[string]any) (Result, error) {
			return Result{Message: "ok"}, nil
		},
	}
	es := NewEventStore(&dispatcher, 256, DropOldest)
	es.Async = true

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx := es.BeginTransaction()
		for j := 0; j < 16; j++ {
			tx.Publish(Event{ID: "t", Projection: "p", Args: map[string]any{}})
		}
		if err := tx.Commit(context.Background()); err != nil {
			b.Fatalf("Commit error: %v", err)
		}
		es.Publish()
		es.Drain(context.Background())
	}
}
