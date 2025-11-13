package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Protocol-Lattice/GoEventBus"
)

// Define a typed projection as a struct
type HouseWasSold struct{}

func main() {
	dispatcher := GoEventBus.Dispatcher{
		"user_created": func(ctx context.Context, args map[string]any) (GoEventBus.Result, error) {
			userID := args["id"].(string)
			fmt.Println("User created with ID:", userID)
			return GoEventBus.Result{Message: "handled user_created"}, nil
		},
		HouseWasSold{}: func(ctx context.Context, args map[string]any) (GoEventBus.Result, error) {
			address := args["address"].(string)
			price := args["price"].(int)
			fmt.Printf("House sold at %s for $%d\n", address, price)
			return GoEventBus.Result{Message: "handled HouseWasSold"}, nil
		},
	}

	store := GoEventBus.NewEventStore(&dispatcher, 1<<16, GoEventBus.DropOldest)
	store.Async = true

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Goroutine: Publish every 5 seconds
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				store.Publish()
				time.Sleep(5 * time.Second)
			}
		}
	}()

	// Goroutine: Subscribe every 6 seconds
	go func() {
		i := 0
		for {
			select {
			case <-ctx.Done():
				return
			default:
				eventType := ""
				var projection any
				var args map[string]any

				if i%2 == 0 {
					eventType = "user_created"
					projection = "user_created"
					args = map[string]any{"id": fmt.Sprintf("%d", i)}
				} else {
					eventType = "HouseWasSold"
					projection = HouseWasSold{}
					args = map[string]any{
						"address": fmt.Sprintf("%d Main St", 100+i),
						"price":   400000 + (i * 1000),
					}
				}

				err := store.Subscribe(context.Background(), GoEventBus.Event{
					ID:         fmt.Sprintf("evt%d", i),
					Projection: projection,
					Args:       args,
				})
				if err != nil {
					log.Printf("Failed to subscribe event (%s): %v", eventType, err)
				} else {
					log.Printf("Subscribed event: %s", eventType)
				}

				i++
				time.Sleep(6 * time.Second)
			}
		}
	}()

	// Wait for Ctrl+C
	<-ctx.Done()
	fmt.Println("Shutting down...")

	// After stop signal: Drain and report metrics
	store.Publish()
	if err := store.Drain(context.Background()); err != nil {
		log.Fatalf("Failed to drain EventStore: %v", err)
	}

	published, processed, errors := store.Metrics()
	fmt.Printf("published=%d processed=%d errors=%d\n", published, processed, errors)
}
