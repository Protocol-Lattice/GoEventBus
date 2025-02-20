package main

import (
	"fmt"
	"log"

	gbus "github.com/Raezil/GoEventBus"

	_ "github.com/lib/pq"
)

// HouseWasSold represents an event for when a house has been sold
type HouseWasSold struct{}

// NewDispatcher initializes the dispatcher with event handlers
func NewDispatcher() *gbus.Dispatcher {
	return &gbus.Dispatcher{
		HouseWasSold{}: func(m map[string]interface{}) (gbus.Result, error) {
			price, ok := m["price"].(int)
			if !ok {
				return gbus.Result{}, fmt.Errorf("price not provided or invalid")
			}
			result := fmt.Sprintf("House was sold for %d", price)
			log.Println(result)
			return gbus.Result{
				Message: result,
			}, nil
		},
	}
}

func main() {
	// Initialize dispatcher and event store
	dispatcher := NewDispatcher()
	eventstore := gbus.NewEventStore(dispatcher)
	eventstore.Publish(gbus.NewEvent(
		HouseWasSold{},
		map[string]interface{}{
			"price": 100,
		},
	))

	eventstore.Publish(gbus.NewEvent(
		HouseWasSold{},
		map[string]interface{}{
			"price": 100,
		},
	))
	// Broadcast the event
	eventstore.Broadcast()

}
