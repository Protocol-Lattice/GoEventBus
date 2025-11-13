package main

import (
	"context"
	"fmt"

	"github.com/Protocol-Lattice/GoEventBus"
)

// Demonstrates OverrunPolicy=ReturnError (fail fast).
func main() {
	dispatcher := GoEventBus.Dispatcher{}
	store := GoEventBus.NewEventStore(&dispatcher, 2, GoEventBus.ReturnError)

	_ = store.Subscribe(context.Background(), GoEventBus.Event{ID: "R-0"})
	_ = store.Subscribe(context.Background(), GoEventBus.Event{ID: "R-1"}) // buffer full

	err := store.Subscribe(context.Background(), GoEventBus.Event{ID: "R-2"})
	fmt.Println("Subscribe error:", err)
}
