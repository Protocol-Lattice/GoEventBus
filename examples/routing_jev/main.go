package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	GoEventBus "github.com/Protocol-Lattice/GoEventBus"
)

func main() {
	apiKey := os.Getenv("OPENROUTER_API_KEY")
	if apiKey == "" {
		fmt.Println("set OPENROUTER_API_KEY to run this example")
		return
	}

	ctx := context.Background()

	dispatcher := GoEventBus.Dispatcher{}
	dispatcher.Register("user.created", printHandler)
	dispatcher.Register("house.sold", printHandler)
	dispatcher.Register("order.cancelled", printHandler)

	store := GoEventBus.NewEventStore(&dispatcher, 16, GoEventBus.DropOldest)
	defer func() { _ = store.Close(context.Background()) }()

	selector := &GoEventBus.RuleCacheSelector{
		Rules: []GoEventBus.EventRule{
			{
				Name:   "explicit-cancel",
				Choice: "order_cancelled",
				Match: func(
					_ context.Context,
					state any,
					_ []GoEventBus.EventCandidate,
				) bool {
					input, ok := state.(map[string]any)
					return ok && input["action"] == "cancel"
				},
			},
		},
		Cache: GoEventBus.NewMemoryDecisionCache(5 * time.Minute),
		Fallback: &GoEventBus.JevSelector{
			APIKey: apiKey,
		},
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
		{
			Key:         "order_cancelled",
			Projection:  "order.cancelled",
			Description: "An existing order should be cancelled",
		},
	}

	state := map[string]any{
		"message": "The property at 1 Main St was sold for $500000",
	}

	decision, err := store.DecideAndSubscribe(
		ctx,
		selector,
		state,
		GoEventBus.Event{
			ID:   "evt-jev-1",
			Data: state,
		},
		candidates,
	)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf(
		"selected=%s confidence=%.2f probabilities=%v\n",
		decision.Choice,
		decision.Confidence,
		decision.Probabilities,
	)

	store.Publish()
}

func printHandler(
	_ context.Context,
	ev GoEventBus.Event,
) (GoEventBus.Result, error) {
	fmt.Printf("handled projection=%v event=%s\n", ev.Projection, ev.ID)
	return GoEventBus.Result{Message: "ok"}, nil
}
