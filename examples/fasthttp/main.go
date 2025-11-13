// main.go
package main

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/Protocol-Lattice/GoEventBus"
	"github.com/fasthttp/router"
	"github.com/valyala/fasthttp"
)

func main() {
	// 1) Setup your dispatcher
	dispatcher := GoEventBus.Dispatcher{
		"sayHello": func(ctx context.Context, args map[string]any) (GoEventBus.Result, error) {
			fmt.Printf("[%s] Hello, %s!\n",
				time.Now().Format(time.StampMilli),
				args["name"],
			)
			return GoEventBus.Result{}, nil
		},
		"computeSum": func(ctx context.Context, args map[string]any) (GoEventBus.Result, error) {
			a := args["a"].(int)
			b := args["b"].(int)
			sum := a + b
			fmt.Printf("[%s] %d + %d = %d\n",
				time.Now().Format(time.StampMilli),
				a, b, sum,
			)
			return GoEventBus.Result{Message: strconv.Itoa(sum)}, nil
		},
	}

	// 2) Create the EventStore
	store := GoEventBus.NewEventStore(&dispatcher, 1<<16, GoEventBus.DropOldest)
	store.Async = false // synchronous dispatch on Publish()

	// 3) HTTP router only enqueues
	r := router.New()
	r.GET("/enqueue", func(ctx *fasthttp.RequestCtx) {
		name := string(ctx.QueryArgs().Peek("name"))
		a, _ := ctx.QueryArgs().GetUint("a")
		b, _ := ctx.QueryArgs().GetUint("b")

		// enqueue two events
		store.Subscribe(context.Background(), GoEventBus.Event{
			ID:         fmt.Sprintf("hello-%d", time.Now().UnixNano()),
			Projection: "sayHello",
			Args:       map[string]any{"name": name},
		})
		store.Subscribe(context.Background(), GoEventBus.Event{
			ID:         fmt.Sprintf("sum-%d", time.Now().UnixNano()),
			Projection: "computeSum",
			Args:       map[string]any{"a": int(a), "b": int(b)},
		})

		ctx.SetStatusCode(fasthttp.StatusAccepted)
		ctx.WriteString("✅ Events enqueued\n")
	})

	// 4) Start HTTP server
	go func() {
		log.Println("▶️  Listening on :8080")
		if err := fasthttp.ListenAndServe(":8080", r.Handler); err != nil {
			log.Fatalf("HTTP error: %v", err)
		}
	}()

	// 5) Background for‐loop: publish every 5 seconds
	go func() {
		for {
			time.Sleep(5 * time.Second)
			store.Publish()
		}
	}()

	// 6) Block forever (or replace with signal handling if you like)
	select {}
}
