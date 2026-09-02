// Command example exercises every BlackHole pattern from Go against a running server.
//
// BlackHole Messaging - Gravicode Studios, led by Kang Fadhil.
//
// Start a server first, for instance:
//
//	dotnet run --project tests/BlackHole.InteropServer -- --port 5000
//
// then:
//
//	go run ./example -addr 127.0.0.1:5000
package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	bh "github.com/DotNetVibeCoderz/Vibe_Messaging/BlackHole/clients/go/v3/blackhole"
)

func main() {
	address := flag.String("addr", "127.0.0.1:5000", "server address")
	flag.Parse()

	ctx := context.Background()

	client, err := bh.ConnectWithRetry(ctx, *address, 5, 100*time.Millisecond, &bh.Options{
		Configure: func(c *bh.Client) {
			// Registered before the read loop starts, so a server that calls back the instant it
			// accepts cannot beat this registration.
			c.RegisterText("client/identify", func(question string) string {
				return "go-example:" + question
			})
		},
	})
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer client.Close()

	fmt.Println("connected to", *address)

	// --- RPC ---------------------------------------------------------------
	shouted, err := client.CallText(ctx, "upper", "halo blackhole")
	if err != nil {
		log.Fatalf("upper: %v", err)
	}
	fmt.Printf("rpc        : upper(%q) -> %q\n", "halo blackhole", shouted)

	if _, err := client.Call(ctx, "does-not-exist", nil); err != nil {
		fmt.Printf("rpc error  : %v\n", err)
	}

	// --- Pub/Sub -----------------------------------------------------------
	delivered := make(chan string, 4)
	if err := client.Subscribe("sensor/+/temperature", func(topic string, payload []byte) {
		delivered <- fmt.Sprintf("%s = %s", topic, payload)
	}); err != nil {
		log.Fatalf("subscribe: %v", err)
	}
	time.Sleep(300 * time.Millisecond)

	_ = client.PublishText("sensor/tank-3/temperature", "28.4")
	_ = client.PublishText("sensor/tank-3/humidity", "62") // matches no filter

	select {
	case line := <-delivered:
		fmt.Println("pubsub     :", line)
	case <-time.After(3 * time.Second):
		fmt.Println("pubsub     : nothing arrived")
	}

	// --- Streaming ---------------------------------------------------------
	payload := bytes.Repeat([]byte("blackhole"), 128*1024) // ~1.1 MiB
	started := time.Now()
	sent, err := client.SendStream(ctx, "example-upload", bytes.NewReader(payload),
		bh.StreamDescriptor{Name: "example.bin", TotalLength: int64(len(payload))},
		16*1024, nil)
	if err != nil {
		log.Fatalf("stream: %v", err)
	}
	fmt.Printf("streaming  : %.1f MiB in %v\n", float64(sent)/(1024*1024), time.Since(started).Round(time.Millisecond))

	// --- Batching ----------------------------------------------------------
	const batchSize = 1000
	messages := make([]bh.Message, batchSize)
	for i := range messages {
		messages[i] = bh.Message{
			Type:    bh.TypePublish,
			Header:  "log/entry",
			Payload: []byte(fmt.Sprintf("line %d", i)),
		}
	}
	started = time.Now()
	if err := client.SendBatch(messages); err != nil {
		log.Fatalf("batch: %v", err)
	}
	fmt.Printf("batching   : %d messages in one write, %v\n", batchSize, time.Since(started).Round(time.Microsecond))

	// --- Server calling back into this client ------------------------------
	answer, err := client.CallText(ctx, "callback", "hello")
	if err != nil {
		fmt.Printf("callback   : %v\n", err)
	} else {
		fmt.Printf("callback   : server asked, client answered %q\n", answer)
	}

	// --- Connection --------------------------------------------------------
	// Averaged over 50 probes: time.Now on Windows resolves to about 500us, so a single loopback
	// round trip falls inside one tick and would read as zero.
	if average, err := client.PingAverage(ctx, 50); err == nil {
		fmt.Printf("keepalive  : %v average round trip over 50 probes\n", average.Round(time.Microsecond))
	}
	fmt.Println("statistics :", client.Stats)
	fmt.Println()
	fmt.Println("Gravicode Studios - led by Kang Fadhil")
}
