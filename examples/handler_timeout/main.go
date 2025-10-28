package main

import (
	"context"
	"fmt"
	"time"

	"github.com/Protocol-Lattice/GoEventBus"
)

// Handler runs for 150 ms unless its context is cancelled.
func main() {
	dispatcher := GoEventBus.Dispatcher{
		"demo": func(ctx context.Context, _ map[string]any) (GoEventBus.Result, error) {
			select {
			case <-time.After(150 * time.Millisecond):
				fmt.Println("handler OK")
				return GoEventBus.Result{Message: "ok"}, nil
			case <-ctx.Done():
				fmt.Println("handler cancelled:", ctx.Err())
				return GoEventBus.Result{}, ctx.Err()
			}
		},
	}

	store := GoEventBus.NewEventStore(&dispatcher, 4, GoEventBus.Block)

	// Handler‑only 40 ms timeout → will cancel.
	hctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()

	_ = store.Subscribe(context.Background(), GoEventBus.Event{
		ID: "H-1", Projection: "demo", Args: map[string]any{"__ctx": hctx},
	})

	store.Publish()
	time.Sleep(60 * time.Millisecond)

	pub, proc, errs := store.Metrics()
	fmt.Printf("published=%d processed=%d errors=%d\n", pub, proc, errs)
}
