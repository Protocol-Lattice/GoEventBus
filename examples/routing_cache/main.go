package main

import (
	"context"
	"fmt"

	GoEventBus "github.com/Protocol-Lattice/GoEventBus"
)

type countingSelector struct {
	calls int
}

func (s *countingSelector) SelectEvent(
	_ context.Context,
	_ any,
	_ []GoEventBus.EventCandidate,
) (GoEventBus.EventDecision, error) {
	s.calls++
	return GoEventBus.EventDecision{
		Choice:     "house_sold",
		Confidence: 0.94,
	}, nil
}

func main() {
	ctx := context.Background()

	dispatcher := GoEventBus.Dispatcher{}
	dispatcher.Register("house.sold", func(
		ctx context.Context,
		ev GoEventBus.Event,
	) (GoEventBus.Result, error) {
		fmt.Printf("handled event=%s projection=%v\n", ev.ID, ev.Projection)
		return GoEventBus.Result{Message: "ok"}, nil
	})

	store := GoEventBus.NewEventStore(&dispatcher, 16, GoEventBus.DropOldest)
	defer func() { _ = store.Close(context.Background()) }()

	fallback := &countingSelector{}
	selector := &GoEventBus.RuleCacheSelector{
		Cache:    GoEventBus.NewMemoryDecisionCache(0),
		Fallback: fallback,
	}

	state := map[string]any{
		"message": "The property at 1 Main St was sold",
	}
	candidates := []GoEventBus.EventCandidate{
		{
			Key:         "user_created",
			Projection:  "user.created",
			Description: "A new user account was created",
		},
		{
			Key:         "house_sold",
			Projection:  "house.sold",
			Description: "A property sale was completed",
		},
	}

	for i := 1; i <= 2; i++ {
		decision, err := store.DecideAndSubscribe(
			ctx,
			selector,
			state,
			GoEventBus.Event{
				ID: fmt.Sprintf("evt-cache-%d", i),
			},
			candidates,
		)
		if err != nil {
			panic(err)
		}
		fmt.Printf("decision %d: choice=%s confidence=%.2f\n", i, decision.Choice, decision.Confidence)
	}

	store.Publish()

	fmt.Printf("fallback selector calls=%d (want 1; second decision came from cache)\n", fallback.calls)
}
