package GoEventBus

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

// dummy handler just increments a counter
func makeCounterHandler(counter *uint64) HandlerFunc {
	return func(ctx context.Context, ev Event) (Result, error) {
		atomic.AddUint64(counter, 1)
		return Result{Message: "ok"}, nil
	}
}


type transactionSelectorFunc func(context.Context, any, []EventCandidate) (EventDecision, error)

func (f transactionSelectorFunc) SelectEvent(
	ctx context.Context,
	state any,
	candidates []EventCandidate,
) (EventDecision, error) {
	return f(ctx, state, candidates)
}

func TestTransaction_DecideAndPublishWithJev(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"model":"typesafe/jev-1.13",
			"id":"tx-jev-test",
			"answers":{
				"event_type":{
					"type":"choice",
					"choice":"invoice_created",
					"probabilities":{"invoice_created":0.96,"order_created":0.04},
					"confidence":0.92
				}
			}
		}`))
	}))
	defer server.Close()

	var processed uint64
	var gotProjection any
	dispatcher := Dispatcher{}
	dispatcher.Register("invoice.created", func(_ context.Context, event Event) (Result, error) {
		gotProjection = event.Projection
		atomic.AddUint64(&processed, 1)
		return Result{Message: "ok"}, nil
	})

	store := NewEventStore(&dispatcher, 8, DropOldest)
	t.Cleanup(func() { _ = store.Close(context.Background()) })

	tx := store.BeginTransaction()
	selector := &JevSelector{
		APIKey:   "test-key",
		Endpoint: server.URL,
		Client:   server.Client(),
	}

	decision, err := tx.DecideAndPublish(
		context.Background(),
		selector,
		map[string]any{"message": "create an invoice for order 42"},
		Event{ID: "evt-jev-tx", Data: map[string]any{"order_id": "42"}},
		[]EventCandidate{
			{
				Key:         "order_created",
				Projection:  "order.created",
				Description: "A new order should be created",
			},
			{
				Key:         "invoice_created",
				Projection:  "invoice.created",
				Description: "An invoice should be created",
			},
		},
	)
	if err != nil {
		t.Fatalf("DecideAndPublish: %v", err)
	}
	if decision.Choice != "invoice_created" {
		t.Fatalf("choice = %q, want invoice_created", decision.Choice)
	}
	if decision.Projection != "invoice.created" {
		t.Fatalf("projection = %#v, want invoice.created", decision.Projection)
	}
	if decision.RequestID != "tx-jev-test" {
		t.Fatalf("request ID = %q, want tx-jev-test", decision.RequestID)
	}
	if got := atomic.LoadUint64(&processed); got != 0 {
		t.Fatalf("handler ran before Commit: %d", got)
	}

	if err := tx.Commit(context.Background()); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if got := atomic.LoadUint64(&processed); got != 1 {
		t.Fatalf("processed = %d, want 1", got)
	}
	if gotProjection != "invoice.created" {
		t.Fatalf("handler projection = %#v, want invoice.created", gotProjection)
	}
}

func TestTransaction_DecideAndPublishRollback(t *testing.T) {
	var processed uint64
	dispatcher := Dispatcher{
		"selected": hs(makeCounterHandler(&processed)),
	}
	store := NewEventStore(&dispatcher, 8, DropOldest)
	t.Cleanup(func() { _ = store.Close(context.Background()) })

	selector := transactionSelectorFunc(func(
		context.Context,
		any,
		[]EventCandidate,
	) (EventDecision, error) {
		return EventDecision{Choice: "selected", Confidence: 1}, nil
	})

	tx := store.BeginTransaction()
	if _, err := tx.DecideAndPublish(
		context.Background(),
		selector,
		"state",
		Event{ID: "routed"},
		[]EventCandidate{{Key: "selected", Projection: "selected"}},
	); err != nil {
		t.Fatalf("DecideAndPublish: %v", err)
	}

	tx.Rollback()
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatalf("Commit after Rollback: %v", err)
	}
	if got := atomic.LoadUint64(&processed); got != 0 {
		t.Fatalf("processed = %d after Rollback, want 0", got)
	}
}

func TestTransaction_DecideAndPublishErrorDoesNotBuffer(t *testing.T) {
	var processed uint64
	dispatcher := Dispatcher{
		"known": hs(makeCounterHandler(&processed)),
	}
	store := NewEventStore(&dispatcher, 8, DropOldest)
	t.Cleanup(func() { _ = store.Close(context.Background()) })

	selector := transactionSelectorFunc(func(
		context.Context,
		any,
		[]EventCandidate,
	) (EventDecision, error) {
		return EventDecision{Choice: "unknown"}, nil
	})

	tx := store.BeginTransaction()
	_, err := tx.DecideAndPublish(
		context.Background(),
		selector,
		"state",
		Event{ID: "bad"},
		[]EventCandidate{{Key: "known", Projection: "known"}},
	)
	if !errors.Is(err, ErrUnknownEventChoice) {
		t.Fatalf("error = %v, want ErrUnknownEventChoice", err)
	}

	if err := tx.Commit(context.Background()); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if got := atomic.LoadUint64(&processed); got != 0 {
		t.Fatalf("failed decision buffered an event: processed = %d", got)
	}
}

func TestTransaction_CommitAndRollback(t *testing.T) {
	// set up dispatcher with a no-op projection
	var processed uint64
	dispatcher := Dispatcher{
		"p": hs(makeCounterHandler(&processed)),
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

func TestTransaction_RollbackPreservesIndependentStoreEvents(t *testing.T) {
	var processed uint64
	dispatcher := Dispatcher{
		"p": hs(makeCounterHandler(&processed)),
	}
	store := NewEventStore(&dispatcher, 8, DropOldest)

	tx := store.BeginTransaction()
	tx.Publish(Event{ID: "transactional", Projection: "p"})
	if err := store.Subscribe(context.Background(), Event{ID: "independent", Projection: "p"}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	tx.Rollback()
	store.Publish()

	if got := atomic.LoadUint64(&processed); got != 1 {
		t.Fatalf("Rollback discarded an independent event: processed %d, want 1", got)
	}
}

func TestTransaction_PartialFailure(t *testing.T) {
	// handler that errors on the second event
	cnt := uint64(0)
	dispatcher := Dispatcher{
		"x": hs(func(ctx context.Context, ev Event) (Result, error) {
			i := atomic.AddUint64(&cnt, 1)
			if i == 2 {
				return Result{}, errors.New("boom")
			}
			return Result{Message: "ok"}, nil
		}),
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
		"p": hs(func(ctx context.Context, ev Event) (Result, error) {
			return Result{Message: "ok"}, nil
		}),
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
		"p": hs(func(ctx context.Context, ev Event) (Result, error) {
			return Result{Message: "ok"}, nil
		}),
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
