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
	dispatcher.Register("order.created", printHandler)
	dispatcher.Register("invoice.created", printHandler)

	store := GoEventBus.NewEventStore(&dispatcher, 16, GoEventBus.DropOldest)
	defer func() { _ = store.Close(context.Background()) }()

	selector := &GoEventBus.RuleCacheSelector{
		Cache: GoEventBus.NewMemoryDecisionCache(5 * time.Minute),
		Fallback: &GoEventBus.JevSelector{
			APIKey: apiKey,
		},
	}

	candidates := []GoEventBus.EventCandidate{
		{
			Key:         "order_created",
			Projection:  "order.created",
			Description: "A new order should be created",
		},
		{
			Key:         "invoice_created",
			Projection:  "invoice.created",
			Description: "An invoice should be created for an existing order",
		},
	}

	decision, timer, err := store.DecideAndScheduleAfter(
		ctx,
		2*time.Second,
		selector,
		map[string]any{
			"message": "Create an invoice for order 42 in two seconds",
		},
		GoEventBus.Event{
			ID:   "evt-scheduled-1",
			Data: map[string]any{"order_id": "42"},
		},
		candidates,
	)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf(
		"scheduled choice=%s confidence=%.2f\n",
		decision.Choice,
		decision.Confidence,
	)

	if timer == nil {
		return
	}

	select {
	case <-time.After(3 * time.Second):
	case <-ctx.Done():
		log.Fatal(ctx.Err())
	}
}

func printHandler(
	_ context.Context,
	ev GoEventBus.Event,
) (GoEventBus.Result, error) {
	fmt.Printf("handled projection=%v event=%s\n", ev.Projection, ev.ID)
	return GoEventBus.Result{Message: "ok"}, nil
}
