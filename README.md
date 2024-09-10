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
```go
package main

import (
	"fmt"

	gbus "github.com/Raezil/GoEventBus"
)

// the message entity to be dispatched
type HouseWasSold struct{}

func main() {
	dispatcher := &gbus.Dispatcher{
		"main.HouseWasSold": func(m map[string]any) {
			fmt.Printf("dispatch: %v\n",m)
		},
	}
	eventstore := gbus.NewEventStore(dispatcher)
	eventstore.Publish(gbus.NewEvent(
				HouseWasSold{},
				map[string]any{
					"price": 1 * 100,
				},
			))
	eventstore.Run()
}
```

3. Get the dependency
```sh
go get github.com/Shibbaz/GOEventBus@v0.1.6.2
``` 

4. Run the project
```sh
go run ./
```

Output:
```sh
2024/04/14 16:40:04 Event id of 6da96821-b27a-4db4-8f5f-e7a1e189b813 was published from channel 'd7a3c677-f328-4f76-addc-d11d64cde566'
2024/04/14 16:40:04 Channel a2cb010f-af65-4030-9e1e-44cdbd9baa5a was opened
dispatch: map[price:100]
...
```
