package GoEventBus

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

type eventSelectorFunc func(context.Context, any, []EventCandidate) (EventDecision, error)

func (f eventSelectorFunc) SelectEvent(
	ctx context.Context,
	state any,
	candidates []EventCandidate,
) (EventDecision, error) {
	return f(ctx, state, candidates)
}

func TestEventStore_DecideAndSubscribeRoutesTypedProjection(t *testing.T) {
	type HouseWasSold struct{}

	called := false
	dispatcher := Dispatcher{}
	dispatcher.Register(HouseWasSold{}, func(ctx context.Context, ev Event) (Result, error) {
		called = true
		if _, ok := ev.Projection.(HouseWasSold); !ok {
			t.Fatalf("unexpected projection type: %T", ev.Projection)
		}
		return Result{Message: "ok"}, nil
	})

	store := NewEventStore(&dispatcher, 8, DropOldest)
	store.Async = false
	t.Cleanup(func() { _ = store.Close(context.Background()) })

	selector := eventSelectorFunc(func(
		ctx context.Context,
		state any,
		candidates []EventCandidate,
	) (EventDecision, error) {
		if len(candidates) != 2 {
			t.Fatalf("got %d candidates, want 2", len(candidates))
		}
		return EventDecision{
			Choice:        "house_sold",
			Confidence:    0.93,
			Probabilities: map[string]float64{"house_sold": 0.93, "user_created": 0.07},
		}, nil
	})

	decision, err := store.DecideAndSubscribe(
		context.Background(),
		selector,
		map[string]any{"message": "The house at Main Street was sold"},
		Event{ID: "evt-1"},
		[]EventCandidate{
			{
				Key:         "user_created",
				Projection:  "user_created",
				Description: "A new user account was created",
			},
			{
				Key:         "house_sold",
				Projection:  HouseWasSold{},
				Description: "A property sale was completed",
			},
		},
	)
	if err != nil {
		t.Fatalf("DecideAndSubscribe: %v", err)
	}
	if decision.Choice != "house_sold" {
		t.Fatalf("choice = %q, want house_sold", decision.Choice)
	}
	if _, ok := decision.Projection.(HouseWasSold); !ok {
		t.Fatalf("decision projection type = %T, want HouseWasSold", decision.Projection)
	}

	store.Publish()
	if !called {
		t.Fatal("selected handler was not executed")
	}
}

func TestEventStore_DecideAndSubscribePublishesToConfiguredProvider(t *testing.T) {
	dispatcher := Dispatcher{}
	localCalls := 0
	dispatcher.Register("order.created", func(context.Context, Event) (Result, error) {
		localCalls++
		return Result{}, nil
	})

	provider := &testProvider{}
	store := NewEventStore(&dispatcher, 8, DropOldest, WithProvider(provider))
	t.Cleanup(func() { _ = store.Close(context.Background()) })

	selector := eventSelectorFunc(func(
		context.Context,
		any,
		[]EventCandidate,
	) (EventDecision, error) {
		return EventDecision{Choice: "order_created", Confidence: 0.95}, nil
	})

	decision, err := store.DecideAndSubscribe(
		context.Background(),
		selector,
		map[string]any{"message": "create order 42"},
		Event{ID: "evt-provider", Data: map[string]any{"order_id": "o-42"}},
		[]EventCandidate{{
			Key:         "order_created",
			Projection:  "order.created",
			Description: "Create an order",
		}},
	)
	if err != nil {
		t.Fatalf("DecideAndSubscribe: %v", err)
	}
	if decision.Projection != "order.created" {
		t.Fatalf("decision projection = %#v, want order.created", decision.Projection)
	}
	if provider.published.ID != "evt-provider" {
		t.Fatalf("published event ID = %q, want evt-provider", provider.published.ID)
	}
	if provider.published.Projection != "order.created" {
		t.Fatalf("published projection = %#v, want order.created", provider.published.Projection)
	}

	store.Publish()
	if localCalls != 0 {
		t.Fatalf("local handler calls = %d, want 0 before provider consumption", localCalls)
	}
}

func TestEventStore_DecideAndSubscribeRejectsUnknownChoice(t *testing.T) {
	dispatcher := Dispatcher{}
	store := NewEventStore(&dispatcher, 8, DropOldest)
	t.Cleanup(func() { _ = store.Close(context.Background()) })

	selector := eventSelectorFunc(func(
		context.Context,
		any,
		[]EventCandidate,
	) (EventDecision, error) {
		return EventDecision{Choice: "not_registered"}, nil
	})

	_, err := store.DecideAndSubscribe(
		context.Background(),
		selector,
		"state",
		Event{ID: "evt-1"},
		[]EventCandidate{{Key: "known", Projection: "known"}},
	)
	if !errors.Is(err, ErrUnknownEventChoice) {
		t.Fatalf("error = %v, want ErrUnknownEventChoice", err)
	}
}

func TestJevSelector_SelectEvent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-key" {
			t.Errorf("authorization = %q", got)
		}

		var request jevDecisionRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode request: %v", err)
		}
		if request.Model != defaultJevModel {
			t.Errorf("model = %q, want %q", request.Model, defaultJevModel)
		}
		question, ok := request.Questions["event_type"]
		if !ok {
			t.Error("missing event_type question")
		}
		if question.Type != "choice" {
			t.Errorf("question type = %q, want choice", question.Type)
		}
		if question.Criteria["order_created"] != "A new order should be created" {
			t.Errorf("unexpected criteria: %#v", question.Criteria)
		}

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"model":"typesafe/jev-1.13-20260917",
			"id":"gen-dec-test",
			"answers":{
				"event_type":{
					"type":"choice",
					"choice":"order_created",
					"probabilities":{
						"order_created":0.91,
						"order_cancelled":0.09
					},
					"confidence":0.82
				}
			}
		}`))
	}))
	defer server.Close()

	selector := &JevSelector{
		APIKey:   "test-key",
		Endpoint: server.URL,
	}

	decision, err := selector.SelectEvent(
		context.Background(),
		map[string]any{"command": "create an order for customer 42"},
		[]EventCandidate{
			{
				Key:         "order_created",
				Projection:  "order.created",
				Description: "A new order should be created",
			},
			{
				Key:         "order_cancelled",
				Projection:  "order.cancelled",
				Description: "An existing order should be cancelled",
			},
		},
	)
	if err != nil {
		t.Fatalf("SelectEvent: %v", err)
	}
	if decision.Choice != "order_created" {
		t.Fatalf("choice = %q, want order_created", decision.Choice)
	}
	if decision.Confidence != 0.82 {
		t.Fatalf("confidence = %v, want 0.82", decision.Confidence)
	}
	if decision.Probabilities["order_created"] != 0.91 {
		t.Fatalf("probability = %v, want 0.91", decision.Probabilities["order_created"])
	}
	if decision.Model != "typesafe/jev-1.13-20260917" {
		t.Fatalf("model = %q", decision.Model)
	}
	if decision.RequestID != "gen-dec-test" {
		t.Fatalf("request id = %q", decision.RequestID)
	}
}
