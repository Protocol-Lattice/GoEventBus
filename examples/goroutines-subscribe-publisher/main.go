package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Raezil/GoEventBus"
)

func main() {
	// 1) Build your dispatcher
	dispatcher := GoEventBus.Dispatcher{
		"user_created": func(ctx context.Context, args map[string]any) (GoEventBus.Result, error) {
			select {
			case <-time.After(500 * time.Millisecond):
				fmt.Println("Processed user_created:", args["id"])
			case <-ctx.Done():
				return GoEventBus.Result{}, ctx.Err()
			}
			return GoEventBus.Result{Message: "done"}, nil
		},
	}

	// 2) Create your store (async handlers)
	store := GoEventBus.NewEventStore(&dispatcher, 1<<10, GoEventBus.Block)
	store.Async = true

	// 3) Prepare a root context that we can cancel on shutdown
	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// ----
	// 4a) Enqueue loop: ticks every 200ms, pushes new events
	go func() {
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()
		count := 0
		for {
			select {
			case <-ticker.C:
				count++
				evt := GoEventBus.Event{
					ID:         fmt.Sprintf("u-%d", count),
					Projection: "user_created",
					Args:       map[string]any{"id": count},
				}
				store.Subscribe(rootCtx, evt)
			case <-rootCtx.Done():
				return
			}
		}
	}()

	// 4b) Dispatch loop: ticks every 100ms, calls Publish()
	go func() {
		ticker := time.NewTicker(1000 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				// fire off whatever's in the ring buffer
				store.Publish()
			case <-rootCtx.Done():
				return
			}
		}
	}()

	// ----
	// 5) Listen for SIGINT/SIGTERM
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs
	log.Println("🔔 Shutdown signal received, stopping new work…")
	cancel() // stop both loops

	// 6) Final publish to catch any last enqueued events
	store.Publish()

	// 7) Drain with timeout so we finish in-flight handlers
	drainCtx, cancelDrain := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelDrain()

	if err := store.Drain(drainCtx); err != nil {
		log.Printf("⚠️  Drain error: %v\n", err)
	} else {
		log.Println("✅ All events processed, exiting.")
	}
}
