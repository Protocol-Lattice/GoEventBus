package main

import (
	"context"
	"fmt"

	"github.com/Raezil/GoEventBus"
)

func main() {
	// 1) Create a dispatcher mapping projections to handlers
	dispatcher := GoEventBus.Dispatcher{
		"say_hello": func(ctx context.Context, args map[string]any) (GoEventBus.Result, error) {
			name := args["name"].(string)
			fmt.Printf("Hello, %s!\n", name)
			return GoEventBus.Result{Message: "greeted"}, nil
		},
	}

	// 2) Initialize the EventStore with 8K buffer, DropOldest policy
	store := GoEventBus.NewEventStore(&dispatcher, 1<<13, GoEventBus.DropOldest)

	// 3) Enqueue an event
	_ = store.Subscribe(context.Background(), GoEventBus.Event{
		ID:         "evt1",
		Projection: "say_hello",
		Args:       map[string]any{"name": "World"},
	})

	// 4) Dispatch and wait
	store.Publish()
}
