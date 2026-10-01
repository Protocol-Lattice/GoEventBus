package main

import (
	"context"
	"fmt"
	"log"

	GoEventBus "github.com/Protocol-Lattice/GoEventBus"
)

func main() {
	ctx := context.Background()

	dispatcher := GoEventBus.Dispatcher{}
	dispatcher.Register("order.cancelled", func(
		ctx context.Context,
		ev GoEventBus.Event,
	) (GoEventBus.Result, error) {
		fmt.Printf("handled %s with payload=%v\n", ev.Projection, ev.Data)
		return GoEventBus.Result{Message: "cancelled"}, nil
	})

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
	}

	state := map[string]any{
		"action":   "cancel",
		"order_id": "ord-42",
	}

	decision, err := store.DecideAndSubscribe(
		ctx,
		selector,
		state,
		GoEventBus.Event{
			ID:   "evt-rule-1",
			Data: state,
		},
		[]GoEventBus.EventCandidate{
			{
				Key:         "order_created",
				Projection:  "order.created",
				Description: "Create a new order",
			},
			{
				Key:         "order_cancelled",
				Projection:  "order.cancelled",
				Description: "Cancel an existing order",
			},
		},
	)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("selected=%s confidence=%.2f\n", decision.Choice, decision.Confidence)
	store.Publish()
}
