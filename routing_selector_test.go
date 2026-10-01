package GoEventBus

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestRuleCacheSelector_RuleWinsBeforeFallback(t *testing.T) {
	var fallbackCalls atomic.Int32

	selector := &RuleCacheSelector{
		Rules: []EventRule{
			{
				Name:   "explicit-cancel",
				Choice: "order_cancelled",
				Match: func(_ context.Context, state any, _ []EventCandidate) bool {
					input, ok := state.(map[string]any)
					return ok && input["action"] == "cancel"
				},
			},
		},
		Cache: NewMemoryDecisionCache(time.Minute),
		Fallback: eventSelectorFunc(func(
			context.Context,
			any,
			[]EventCandidate,
		) (EventDecision, error) {
			fallbackCalls.Add(1)
			return EventDecision{Choice: "order_created"}, nil
		}),
	}

	decision, err := selector.SelectEvent(
		context.Background(),
		map[string]any{"action": "cancel"},
		[]EventCandidate{
			{Key: "order_created", Projection: "order.created"},
			{Key: "order_cancelled", Projection: "order.cancelled"},
		},
	)
	if err != nil {
		t.Fatalf("SelectEvent: %v", err)
	}
	if decision.Choice != "order_cancelled" {
		t.Fatalf("choice = %q, want order_cancelled", decision.Choice)
	}
	if decision.Confidence != 1 {
		t.Fatalf("confidence = %v, want 1", decision.Confidence)
	}
	if fallbackCalls.Load() != 0 {
		t.Fatalf("fallback calls = %d, want 0", fallbackCalls.Load())
	}
}

func TestRuleCacheSelector_CacheAvoidsRepeatedFallback(t *testing.T) {
	var fallbackCalls atomic.Int32

	selector := &RuleCacheSelector{
		Cache: NewMemoryDecisionCache(time.Minute),
		Fallback: eventSelectorFunc(func(
			context.Context,
			any,
			[]EventCandidate,
		) (EventDecision, error) {
			fallbackCalls.Add(1)
			return EventDecision{
				Choice:        "house_sold",
				Confidence:    0.91,
				Probabilities: map[string]float64{"house_sold": 0.91},
			}, nil
		}),
	}

	state := map[string]any{
		"message": "The house at Main Street was sold",
	}
	candidates := []EventCandidate{
		{Key: "user_created", Projection: "user_created", Description: "new user"},
		{Key: "house_sold", Projection: struct{}{}, Description: "property sold"},
	}

	first, err := selector.SelectEvent(context.Background(), state, candidates)
	if err != nil {
		t.Fatalf("first SelectEvent: %v", err)
	}
	if first.Choice != "house_sold" {
		t.Fatalf("first choice = %q, want house_sold", first.Choice)
	}

	second, err := selector.SelectEvent(
		context.Background(),
		state,
		[]EventCandidate{candidates[1], candidates[0]},
	)
	if err != nil {
		t.Fatalf("second SelectEvent: %v", err)
	}
	if second.Choice != "house_sold" {
		t.Fatalf("second choice = %q, want house_sold", second.Choice)
	}
	if fallbackCalls.Load() != 1 {
		t.Fatalf("fallback calls = %d, want 1", fallbackCalls.Load())
	}
}

func TestRuleCacheSelector_DifferentStateMissesCache(t *testing.T) {
	var fallbackCalls atomic.Int32

	selector := &RuleCacheSelector{
		Cache: NewMemoryDecisionCache(time.Minute),
		Fallback: eventSelectorFunc(func(
			context.Context,
			any,
			[]EventCandidate,
		) (EventDecision, error) {
			fallbackCalls.Add(1)
			return EventDecision{Choice: "event_a"}, nil
		}),
	}

	candidates := []EventCandidate{{Key: "event_a", Projection: "event_a"}}
	for _, state := range []any{
		map[string]any{"id": 1},
		map[string]any{"id": 2},
	} {
		if _, err := selector.SelectEvent(context.Background(), state, candidates); err != nil {
			t.Fatalf("SelectEvent: %v", err)
		}
	}

	if fallbackCalls.Load() != 2 {
		t.Fatalf("fallback calls = %d, want 2", fallbackCalls.Load())
	}
}

func TestRuleCacheSelector_RuleRejectsUnknownChoice(t *testing.T) {
	selector := &RuleCacheSelector{
		Rules: []EventRule{
			{
				Name:   "bad-rule",
				Choice: "missing",
				Match: func(context.Context, any, []EventCandidate) bool {
					return true
				},
			},
		},
	}

	_, err := selector.SelectEvent(
		context.Background(),
		nil,
		[]EventCandidate{{Key: "known", Projection: "known"}},
	)
	if !errors.Is(err, ErrUnknownEventChoice) {
		t.Fatalf("error = %v, want ErrUnknownEventChoice", err)
	}
}

func TestDefaultDecisionCacheKey_IsCandidateOrderIndependent(t *testing.T) {
	state := map[string]any{"id": "42"}
	a := []EventCandidate{
		{Key: "b", Description: "second", Projection: struct{}{}},
		{Key: "a", Description: "first", Projection: make(chan int)},
	}
	b := []EventCandidate{a[1], a[0]}

	keyA, err := DefaultDecisionCacheKey(state, a)
	if err != nil {
		t.Fatalf("key A: %v", err)
	}
	keyB, err := DefaultDecisionCacheKey(state, b)
	if err != nil {
		t.Fatalf("key B: %v", err)
	}
	if keyA != keyB {
		t.Fatalf("keys differ: %q != %q", keyA, keyB)
	}
}

func TestMemoryDecisionCache_ClonesProbabilityMap(t *testing.T) {
	cache := NewMemoryDecisionCache(time.Minute)
	decision := EventDecision{
		Choice:        "a",
		Probabilities: map[string]float64{"a": 0.9},
	}

	if err := cache.Set(context.Background(), "key", decision); err != nil {
		t.Fatalf("Set: %v", err)
	}
	decision.Probabilities["a"] = 0.1

	got, ok, err := cache.Get(context.Background(), "key")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !ok {
		t.Fatal("cache miss")
	}
	if got.Probabilities["a"] != 0.9 {
		t.Fatalf("cached probability = %v, want 0.9", got.Probabilities["a"])
	}

	got.Probabilities["a"] = 0.2
	again, ok, err := cache.Get(context.Background(), "key")
	if err != nil || !ok {
		t.Fatalf("second Get: ok=%v err=%v", ok, err)
	}
	if again.Probabilities["a"] != 0.9 {
		t.Fatalf("cache was mutated through Get: %v", again.Probabilities["a"])
	}
}
