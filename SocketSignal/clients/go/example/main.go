// SocketSignal - Go example.
// Start a server first: dotnet run --project ../../../src/SocketSignal.Demo
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/DotNetVibeCoderz/Vibe_Messaging/SocketSignal/clients/go/v2/socketsignal"
)

func main() {
	ctx := context.Background()

	client := socketsignal.New(socketsignal.Options{
		KeepAlive: 10 * time.Second,
		OnDisconnected: func(reason string) {
			fmt.Printf("  -- disconnected: %s\n", reason)
		},
	})

	// A method the server can call on us.
	client.On("serverHello", func(args []json.RawMessage) (any, error) {
		var text string
		if len(args) > 0 {
			_ = json.Unmarshal(args[0], &text)
		}
		fmt.Printf("  <- serverHello: %s\n", text)
		return "go heard you", nil
	})

	if err := client.Connect(ctx, "ws://localhost:8080/ws/"); err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	fmt.Printf("connected as %s\n", client.ClientID())

	var total int
	if err := client.Call(ctx, &total, "sum", 5, 7); err != nil {
		log.Fatal(err)
	}
	fmt.Println("sum(5, 7)      =", total)

	var echoed string
	if err := client.Call(ctx, &echoed, "echo", "hello"); err != nil {
		log.Fatal(err)
	}
	fmt.Println("echo('hello')  =", echoed)

	if err := client.Send(ctx, "echo", "no reply wanted"); err != nil {
		log.Fatal(err)
	}

	var ignored string
	if err := client.Call(ctx, &ignored, "explode", "now"); err != nil {
		fmt.Println("caught         =", err)
	}

	time.Sleep(500 * time.Millisecond)
}
