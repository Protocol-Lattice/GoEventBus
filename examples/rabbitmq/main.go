package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	bus "github.com/Protocol-Lattice/GoEventBus"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	dispatcher := bus.Dispatcher{}
	handled := make(chan struct{}, 1)
	dispatcher.Register("order.created", func(_ context.Context, ev bus.Event) (bus.Result, error) {
		fmt.Println("received from RabbitMQ:", ev.Data)
		select {
		case handled <- struct{}{}:
		default:
		}
		return bus.Result{}, nil
	})

	url := os.Getenv("RABBITMQ_URL")
	if url == "" {
		url = "amqp://guest:guest@localhost:5672/"
	}

	store := bus.NewEventStore(
		&dispatcher,
		1024,
		bus.Block,
		bus.WithRabbitMQ(bus.RabbitMQProviderConfig{
			URL:        url,
			Exchange:   "goeventbus.example",
			Queue:      "goeventbus.example",
			BindingKey: "order.*",
			Consumer:   "example-1",
		}),
	)
	defer store.Close(context.Background())

	consumeErr := make(chan error, 1)
	go func() {
		consumeErr <- store.Consume(ctx)
	}()

	if err := store.PublishToProvider(ctx, bus.Event{
		ID:         "rabbitmq-example-1",
		Projection: "order.created",
		Data:       map[string]any{"order_id": "o-42"},
	}); err != nil {
		log.Fatal(err)
	}

	select {
	case <-handled:
		fmt.Println("RabbitMQ event handled")
	case err := <-consumeErr:
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, bus.ErrProviderClosed) {
			log.Fatal(err)
		}
	case <-ctx.Done():
		log.Fatal(ctx.Err())
	}
}
