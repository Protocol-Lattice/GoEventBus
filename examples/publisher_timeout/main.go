package main

import (
	"context"
	"fmt"
	"time"

	"github.com/Raezil/GoEventBus"
)

// Shows back‑pressure timeout when the buffer is full.
func main() {
	dispatcher := GoEventBus.Dispatcher{}
	store := GoEventBus.NewEventStore(&dispatcher, 1, GoEventBus.Block)

	// Fill the single slot.
	_ = store.Subscribe(context.Background(), GoEventBus.Event{ID: "P-0"})

	// Next publish must wait; we give it only 30 ms.
	pubCtx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	err := store.Subscribe(pubCtx, GoEventBus.Event{ID: "P-1"})
	fmt.Println("Subscribe error:", err)

	pub, proc, errs := store.Metrics()
	fmt.Printf("published=%d processed=%d errors=%d\n", pub, proc, errs)
}
