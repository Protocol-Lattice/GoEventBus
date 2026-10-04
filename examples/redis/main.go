package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	bus "github.com/Protocol-Lattice/GoEventBus"
	"github.com/redis/go-redis/v9"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dispatcher := bus.Dispatcher{}
	handled := make(chan struct{}, 1)
	dispatcher.Register("order.created", func(_ context.Context, ev bus.Event) (bus.Result, error) {
		fmt.Println("received from Redis:", ev.Data)
		select {
		case handled <- struct{}{}:
		default:
		}
		return bus.Result{}, nil
	})

	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	defer client.Close()

	store := bus.NewEventStore(
		&dispatcher,
		1024,
		bus.Block,
		bus.WithRedis(bus.RedisProviderConfig{
			Client:   client,
			Stream:   "goeventbus:example",
			Group:    "goeventbus-example",
			Consumer: "example-1",
			StartID:  "0",
			Block:    250 * time.Millisecond,
		}),
	)
	defer store.Close(context.Background())

	consumeErr := make(chan error, 1)
	go func() {
		consumeErr <- store.Consume(ctx)
	}()

	selector := &bus.RuleCacheSelector{
		Rules: []bus.EventRule{{
			Name:   "order-created",
			Choice: "order_created",
			Match: func(context.Context, any, []bus.EventCandidate) bool {
				return true
			},
		}},
	}

	decision, err := store.DecideAndSubscribe(
		ctx,
		selector,
		map[string]any{"message": "create order o-42"},
		bus.Event{
			ID:   "redis-example-1",
			Data: map[string]any{"order_id": "o-42"},
		},
		[]bus.EventCandidate{{
			Key:         "order_created",
			Projection:  "order.created",
			Description: "A new order should be created",
		}},
	)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("selected:", decision.Choice)

	select {
	case <-handled:
		fmt.Println("Redis event handled")
	case err := <-consumeErr:
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, bus.ErrProviderClosed) {
			log.Fatal(err)
		}
	case <-ctx.Done():
		log.Fatal(ctx.Err())
	}
}
