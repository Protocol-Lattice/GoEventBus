<p align="center">
  <img src="https://github.com/Raezil/GoEventBus/blob/main/95a41958-9fd0-4ff9-9770-c2cfd30affb1.jpg?raw=true">
</p>

[![Go Report Card](https://goreportcard.com/badge/github.com/Raezil/GoEventBus)](https://goreportcard.com/report/github.com/Raezil/GoEventBus)
[![libs.tech recommends](https://libs.tech/project/849363374/badge.svg)](https://libs.tech/project/849363374/goeventbus)

EventBus is a simple and efficient **event-driven system** for Go applications.  
It allows you to **publish** and **subscribe** to events seamlessly.

---

## 📦 Installation

To install the library, run:

```sh
go get github.com/Raezil/GoEventBus
```

---

## 🚀 Quick Start

### 1️⃣ Initialize a New Project

```sh
mkdir eventbus-demo
cd eventbus-demo
go mod init eventbus-demo
```

### 2️⃣ Create `main.go`

```go
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	gbus "github.com/Raezil/GoEventBus"
	"github.com/gorilla/mux"
)

// HouseWasSold represents an event for a house sale.
type HouseWasSold struct{}

// NewDispatcher sets up event handlers.
func NewDispatcher() *gbus.Dispatcher {
	return &gbus.Dispatcher{
		HouseWasSold{}: func(data map[string]interface{}) (gbus.Result, error) {
			price, ok := data["price"].(int)
			if !ok {
				return gbus.Result{}, fmt.Errorf("invalid or missing 'price'")
			}
			message := fmt.Sprintf("House sold for %d!", price)
			log.Println(message)
			return gbus.Result{Message: message}, nil
		},
	}
}

func main() {
	dispatcher := NewDispatcher()
	eventStore := gbus.NewEventStore(dispatcher)

	router := mux.NewRouter()
	router.HandleFunc("/house-sold", func(w http.ResponseWriter, r *http.Request) {
		eventStore.Publish(gbus.NewEvent(HouseWasSold{}, map[string]interface{}{"price": 100}))
		
		if err := eventStore.Broadcast(); err != nil {
			log.Printf("Error broadcasting event: %v", err)
			http.Error(w, "Event processing failed", http.StatusInternalServerError)
			return
		}
		
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "House sold event published"})
	})

	log.Println("Server running on :8080")
	log.Fatal(http.ListenAndServe(":8080", router))
}
```

### 3️⃣ Run Your Application

```sh
go run main.go
```

Now, visiting `http://localhost:8080/house-sold` will trigger the event and process it.

---

## 🐇 RabbitMQ Integration

### 1️⃣ Start RabbitMQ

```sh
docker run -it --rm --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:4.0-management
```

### 2️⃣ Implement Event Publishing in `main.go`

```go
package main

import (
	"fmt"
	"log"
	. "github.com/Raezil/GoEventBus"
)

func NewDispatcher() *RabbitDispatcher {
	return &RabbitDispatcher{
		"HouseWasSold": func(data map[string]interface{}) (Result, error) {
			price, ok := data["price"].(float64)
			if !ok {
				return Result{}, fmt.Errorf("invalid or missing 'price'")
			}
			return Result{Message: fmt.Sprintf("House sold for %.2f", price)}, nil
		},
	}
}

func main() {
	dispatcher := NewDispatcher()
	rabbitStore, err := NewRabbitEventStore(dispatcher, "amqp://guest:guest@localhost:5672/", "events_queue")
	if err != nil {
		log.Fatalf("Failed to initialize RabbitEventStore: %v", err)
	}

	rabbitStore.Publish(&Event{Id: "12345", Projection: "HouseWasSold", Args: map[string]interface{}{"price": 100.0}})
	rabbitStore.Publish(&Event{Id: "123456", Projection: "HouseWasSold", Args: map[string]interface{}{"price": 200.0}})

	go rabbitStore.Broadcast()
	select {}
}
```

### 3️⃣ Run Your Application

```sh
go run main.go
```

---

## **📜 Contributing**
Want to improve GoEventBus? 🚀  
1. Fork the repo  
2. Create a feature branch (`git checkout -b feature-new`)  
3. Commit your changes (`git commit -m "Added feature"`)  
4. Push to your branch (`git push origin feature-new`)  
5. Submit a PR!  



## 📖 References

- [GoEventBus GitHub Repository](https://github.com/Raezil/GoEventBus)
- [RabbitMQ Official Documentation](https://www.rabbitmq.com/)
- [Event Sourcing Overview](https://martinfowler.com/eaaDev/EventSourcing.html)
