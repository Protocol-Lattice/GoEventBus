package main

import (
    "context"
    "fmt"
    "time"

    "github.com/Raezil/GoEventBus"
)

// OverrunPolicy=DropOldest silently discards events.
func main() {
    dispatcher := GoEventBus.Dispatcher{
        "noop": func(ctx context.Context, _ map[string]any) (GoEventBus.Result, error) {
            time.Sleep(80 * time.Millisecond)
            return GoEventBus.Result{}, nil
        },
    }
    store := GoEventBus.NewEventStore(&dispatcher, 2, GoEventBus.DropOldest)

    for i := 0; i < 5; i++ {
        _ = store.Subscribe(context.Background(), GoEventBus.Event{
            ID: fmt.Sprintf("D-%d", i),
            Projection: "noop",
        })
    }

    store.Publish()
    time.Sleep(200 * time.Millisecond)

    pub, proc, errs := store.Metrics()
    fmt.Printf("published=%d processed=%d errors=%d\n", pub, proc, errs)
}
