<p align="center">
  <img src="https://github.com/user-attachments/assets/021ebc5a-5d41-49ab-a281-129782bc4a5a">
</p>
<h1 align="center">EventBus lib in Golang!</h1>
> Simple event source system<br /><br />

This project lets you publish and subscribe events easily.

To download:
```
go get github.com/Raezil/GoEventBus
```

# Quick Start
Let's make a pub/sub application:
1. Create a project
```sh
mkdir demo
cd demo
go mod init demo
```

2. Add `main.go`
# Example 1.
```
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


# Example 2
run RabbitMQ
```
docker run -it --rm --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:4.0-management
```
then run go run main.go
```
package main

import (
	"fmt"
	"log"
	. "github.com/Raezil/GoEventBus"
)

func NewDispatcher() *RabbitDispatcher {
	return &RabbitDispatcher{
		"HouseWasSold": func(m map[string]interface{}) (Result, error) {
			price, ok := m["price"].(float64)
			if !ok {
				return Result{}, fmt.Errorf("price not provided or invalid")
			}

			result := fmt.Sprintf("House was sold for %.2f", price)
			return Result{Message: result}, nil
		},
	}
}

func main() {
	dispatcher := NewDispatcher()

	rabbitStore, err := NewRabbitEventStore(dispatcher, "amqp://guest:guest@localhost:5672/", "events_queue")
	if err != nil {
		log.Fatalf("Failed to initialize RabbitEventStore: %v", err)
	}

	rabbitStore.Publish(&Event{
		Id:         "12345",
		Projection: "HouseWasSold",
		Args: map[string]interface{}{
			"price": 100.0,
		},
	})
	rabbitStore.Publish(&Event{
		Id:         "123456",
		Projection: "HouseWasSold",
		Args: map[string]interface{}{
			"price": 200.0,
		},
	})

	go rabbitStore.Broadcast()

	select {}
}
```
3. Get the dependency
```sh
go get github.com/Raezil/GoEventBus
``` 

4. Run the project
```sh
go run ./
```
