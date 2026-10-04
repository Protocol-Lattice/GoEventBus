package GoEventBus

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestDecideAndScheduleAfterWithJev(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"model":"typesafe/jev-1.13",
			"id":"schedule-jev-test",
			"answers":{
				"event_type":{
					"type":"choice",
					"choice":"invoice_created",
					"probabilities":{
						"invoice_created":0.97,
						"order_created":0.03
					},
					"confidence":0.94
				}
			}
		}`))
	}))
	defer server.Close()

	delivered := make(chan Event, 1)
	dispatcher := Dispatcher{}
	dispatcher.Register("invoice.created", func(_ context.Context, event Event) (Result, error) {
		delivered <- event
		return Result{Message: "ok"}, nil
	})

	store := NewEventStore(&dispatcher, 8, DropOldest)
	t.Cleanup(func() { _ = store.Close(context.Background()) })

	selector := &JevSelector{
		APIKey:   "test-key",
		Endpoint: server.URL,
		Client:   server.Client(),
	}

	decision, timer, err := store.DecideAndScheduleAfter(
		context.Background(),
		75*time.Millisecond,
		selector,
		map[string]any{"message": "create an invoice for order 42"},
		Event{ID: "scheduled-1", Data: map[string]any{"order_id": "42"}},
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
		t.Fatalf("DecideAndScheduleAfter: %v", err)
	}
	if timer == nil {
		t.Fatal("timer = nil, want scheduled timer")
	}
	if decision.Choice != "invoice_created" {
		t.Fatalf("choice = %q, want invoice_created", decision.Choice)
	}
	if decision.Projection != "invoice.created" {
		t.Fatalf("projection = %#v, want invoice.created", decision.Projection)
	}
	if decision.RequestID != "schedule-jev-test" {
		t.Fatalf("request ID = %q, want schedule-jev-test", decision.RequestID)
	}

	select {
	case event := <-delivered:
		t.Fatalf("event fired before delay: %#v", event)
	case <-time.After(20 * time.Millisecond):
	}

	select {
	case event := <-delivered:
		if event.ID != "scheduled-1" {
			t.Fatalf("event ID = %q, want scheduled-1", event.ID)
		}
		if event.Projection != "invoice.created" {
			t.Fatalf("event projection = %#v, want invoice.created", event.Projection)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for scheduled event")
	}
}

func TestDecideAndSchedulePastTimeExecutesSelectedEventImmediately(t *testing.T) {
	var processed uint64
	dispatcher := Dispatcher{}
	dispatcher.Register("selected", func(_ context.Context, event Event) (Result, error) {
		if event.ID != "immediate-1" {
			t.Fatalf("event ID = %q, want immediate-1", event.ID)
		}
		atomic.AddUint64(&processed, 1)
		return Result{}, nil
	})

	store := NewEventStore(&dispatcher, 8, DropOldest)
	t.Cleanup(func() { _ = store.Close(context.Background()) })

	selector := eventSelectorFunc(func(
		context.Context,
		any,
		[]EventCandidate,
	) (EventDecision, error) {
		return EventDecision{Choice: "selected", Confidence: 1}, nil
	})

	decision, timer, err := store.DecideAndSchedule(
		context.Background(),
		time.Now().Add(-time.Second),
		selector,
		"state",
		Event{ID: "immediate-1"},
		[]EventCandidate{{Key: "selected", Projection: "selected"}},
	)
	if err != nil {
		t.Fatalf("DecideAndSchedule: %v", err)
	}
	if timer != nil {
		t.Fatal("timer != nil for past schedule")
	}
	if decision.Projection != "selected" {
		t.Fatalf("projection = %#v, want selected", decision.Projection)
	}
	if got := atomic.LoadUint64(&processed); got != 1 {
		t.Fatalf("processed = %d, want 1", got)
	}
}

func TestDecideAndScheduleAfterNonPositiveExecutesImmediately(t *testing.T) {
	var processed uint64
	dispatcher := Dispatcher{
		"selected": hs(makeCounterHandler(&processed)),
	}
	store := NewEventStore(&dispatcher, 8, DropOldest)
	t.Cleanup(func() { _ = store.Close(context.Background()) })

	selector := eventSelectorFunc(func(
		context.Context,
		any,
		[]EventCandidate,
	) (EventDecision, error) {
		return EventDecision{Choice: "selected"}, nil
	})

	_, timer, err := store.DecideAndScheduleAfter(
		context.Background(),
		0,
		selector,
		nil,
		Event{ID: "immediate-after"},
		[]EventCandidate{{Key: "selected", Projection: "selected"}},
	)
	if err != nil {
		t.Fatalf("DecideAndScheduleAfter: %v", err)
	}
	if timer != nil {
		t.Fatal("timer != nil for non-positive duration")
	}
	if got := atomic.LoadUint64(&processed); got != 1 {
		t.Fatalf("processed = %d, want 1", got)
	}
}

func TestDecideAndScheduleSelectionFailureCreatesNoTimer(t *testing.T) {
	store := NewEventStore(&Dispatcher{}, 8, DropOldest)
	t.Cleanup(func() { _ = store.Close(context.Background()) })

	selector := eventSelectorFunc(func(
		context.Context,
		any,
		[]EventCandidate,
	) (EventDecision, error) {
		return EventDecision{Choice: "unknown"}, nil
	})

	_, timer, err := store.DecideAndScheduleAfter(
		context.Background(),
		time.Hour,
		selector,
		"state",
		Event{ID: "never-scheduled"},
		[]EventCandidate{{Key: "known", Projection: "known"}},
	)
	if !errors.Is(err, ErrUnknownEventChoice) {
		t.Fatalf("error = %v, want ErrUnknownEventChoice", err)
	}
	if timer != nil {
		t.Fatal("timer created for failed selection")
	}
}
