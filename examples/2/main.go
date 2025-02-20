package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	gbus "github.com/Raezil/GoEventBus"

	"github.com/gorilla/mux"
	_ "github.com/lib/pq"
)

// HouseWasSold represents an event for when a house has been sold
type HouseWasSold struct{}

// NewDispatcher initializes the dispatcher with event handlers
func NewDispatcher() *gbus.Dispatcher {
	return &gbus.Dispatcher{
		HouseWasSold{}: func(m map[string]interface{}) (gbus.Result, error) {
			price, ok := m["price"].(int) // Match the correct key "price"
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

	router := mux.NewRouter()
	router.HandleFunc("/house-sold", func(w http.ResponseWriter, r *http.Request) {
		// Publish the event with the correct key "price"
		eventstore.Publish(gbus.NewEvent(
			HouseWasSold{},
			map[string]interface{}{
				"price": 100,
			},
		))

		// Broadcast the event after publishing, wait for completion
		if err := eventstore.Broadcast(); err != nil {
			log.Printf("Error broadcasting event: %v", err)
			http.Error(w, "Failed to process event", http.StatusInternalServerError)
			return
		}

		// Send response back to client
		w.Header().Set("Content-Type", "application/json")
		response := map[string]string{"status": "House sold event published"}
		json.NewEncoder(w).Encode(response)
	})

	serverAddress := ":8080"
	log.Printf("Server is listening on %s", serverAddress)
	if err := http.ListenAndServe(serverAddress, router); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
