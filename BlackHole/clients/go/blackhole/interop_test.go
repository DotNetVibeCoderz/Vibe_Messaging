// BlackHole Messaging - Gravicode Studios, led by Kang Fadhil.
//
// Interop against the real .NET server.
//
// Every test here talks to tests/BlackHole.InteropServer, which is the actual library. If the Go
// codec and the C# codec ever disagree by a single byte, these fail. Run them with:
//
//	go test ./blackhole/ -run Interop -v
//
// They are skipped in short mode, since they need a .NET SDK and a built server.

package blackhole_test

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	bh "github.com/DotNetVibeCoderz/Vibe_Messaging/BlackHole/clients/go/v3/blackhole"
)

var (
	serverOnce sync.Once
	serverPort int
	serverErr  error
	serverCmd  *exec.Cmd
)

// interopServer starts the .NET reference peer once per test binary and returns its port.
func interopServer(t *testing.T) int {
	t.Helper()
	if testing.Short() {
		t.Skip("interop needs the .NET server; skipped in short mode")
	}

	serverOnce.Do(func() {
		repoRoot, err := filepath.Abs(filepath.Join("..", "..", ".."))
		if err != nil {
			serverErr = err
			return
		}
		project := filepath.Join(repoRoot, "tests", "BlackHole.InteropServer")

		// Prefer an already-built binary; fall back to dotnet run.
		var command *exec.Cmd
		built := filepath.Join(project, "bin", "Release", "net10.0", "BlackHole.InteropServer.exe")
		if _, statErr := os.Stat(built); statErr == nil {
			command = exec.Command(built, "--port", "0")
		} else {
			command = exec.Command("dotnet", "run", "--project", project, "-c", "Release", "--", "--port", "0")
		}
		command.Dir = repoRoot

		stdout, err := command.StdoutPipe()
		if err != nil {
			serverErr = err
			return
		}
		if err := command.Start(); err != nil {
			serverErr = fmt.Errorf("start interop server: %w", err)
			return
		}
		serverCmd = command

		ready := make(chan int, 1)
		go func() {
			scanner := bufio.NewScanner(stdout)
			for scanner.Scan() {
				line := scanner.Text()
				if strings.HasPrefix(line, "READY ") {
					port, convErr := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, "READY ")))
					if convErr == nil {
						ready <- port
					}
					return
				}
			}
		}()

		select {
		case port := <-ready:
			serverPort = port
		case <-time.After(120 * time.Second):
			serverErr = fmt.Errorf("the interop server did not report a port within 120s")
		}
	})

	if serverErr != nil {
		t.Fatalf("interop server: %v", serverErr)
	}
	return serverPort
}

func TestMain(m *testing.M) {
	code := m.Run()
	if serverCmd != nil && serverCmd.Process != nil {
		_ = serverCmd.Process.Kill()
		_ = serverCmd.Wait()
	}
	os.Exit(code)
}

func connect(t *testing.T, options *bh.Options) *bh.Client {
	t.Helper()
	port := interopServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	client, err := bh.Connect(ctx, fmt.Sprintf("127.0.0.1:%d", port), options)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func TestInteropEchoReturnsTheExactBytes(t *testing.T) {
	client := connect(t, nil)

	payload := make([]byte, 256)
	for i := range payload {
		payload[i] = byte(i)
	}

	result, err := client.Call(context.Background(), "echo", payload)
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if string(result) != string(payload) {
		t.Error("echo did not return the request bytes unchanged")
	}
}

func TestInteropTextRoundTrip(t *testing.T) {
	client := connect(t, nil)

	got, err := client.CallText(context.Background(), "upper", "halo blackhole")
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if got != "HALO BLACKHOLE" {
		t.Errorf("got %q", got)
	}
}

func TestInteropNonASCIISurvivesTheRoundTrip(t *testing.T) {
	client := connect(t, nil)

	// echo, not upper: casing rules differ between runtimes, and what matters is that the UTF-8
	// bytes cross unchanged in both directions.
	original := "suhu tangki 28,4 °C — αβγ — 日本語 — 🕳"
	result, err := client.Call(context.Background(), "echo", []byte(original))
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if string(result) != original {
		t.Errorf("got %q", result)
	}
}

func TestInteropNumericPayload(t *testing.T) {
	client := connect(t, nil)

	result, err := client.Call(context.Background(), "sum", []byte{1, 2, 3, 4, 5})
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	total, ok := bh.Int32(result)
	if !ok || total != 15 {
		t.Errorf("got %d (ok=%v), want 15", total, ok)
	}
}

func TestInteropConcurrentCallsStayCorrelated(t *testing.T) {
	client := connect(t, nil)

	const count = 200
	results := make([]string, count)
	errs := make([]error, count)

	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			results[index], errs[index] = client.CallText(
				context.Background(), "upper", fmt.Sprintf("call-%d", index))
		}(i)
	}
	wg.Wait()

	for i := 0; i < count; i++ {
		if errs[i] != nil {
			t.Fatalf("call %d: %v", i, errs[i])
		}
		if want := fmt.Sprintf("CALL-%d", i); results[i] != want {
			t.Errorf("call %d: got %q, want %q", i, results[i], want)
		}
	}
}

func TestInteropHandlerFailureSurfaces(t *testing.T) {
	client := connect(t, nil)

	_, err := client.Call(context.Background(), "boom", nil)
	if err == nil {
		t.Fatal("expected an error")
	}
	var rpcErr *bh.RPCError
	if !strings.Contains(err.Error(), "boom") {
		t.Errorf("unexpected message: %v", err)
	}
	if ok := asRPCError(err, &rpcErr); !ok || rpcErr.Method != "boom" {
		t.Errorf("expected an RPCError for 'boom', got %v", err)
	}
}

func TestInteropUnknownMethodFailsFast(t *testing.T) {
	client := connect(t, nil)

	start := time.Now()
	_, err := client.Call(context.Background(), "no-such-method", nil)
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(err.Error(), "Unknown method") {
		t.Errorf("unexpected message: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("took %v; an unknown method should fail immediately", elapsed)
	}
}

func TestInteropDeadlineIsEnforced(t *testing.T) {
	client := connect(t, &bh.Options{CallTimeout: 300 * time.Millisecond})

	_, err := client.CallText(context.Background(), "sleep", "30000")
	if err == nil {
		t.Fatal("expected a deadline error")
	}
	if !strings.Contains(err.Error(), "deadline") {
		t.Errorf("unexpected message: %v", err)
	}
}

func TestInteropLargePayloadsCrossIntact(t *testing.T) {
	client := connect(t, nil)

	for _, size := range []int{1, 1024, 64 * 1024, 1024 * 1024} {
		result, err := client.Call(context.Background(), "big", []byte(strconv.Itoa(size)))
		if err != nil {
			t.Fatalf("size %d: %v", size, err)
		}
		if len(result) != size {
			t.Fatalf("size %d: got %d bytes", size, len(result))
		}
		for i := 0; i < size; i++ {
			if result[i] != byte(i%251) {
				t.Fatalf("size %d: byte %d is %d", size, i, result[i])
			}
		}
	}
}

func TestInteropServerCanCallBackIntoTheClient(t *testing.T) {
	client := connect(t, &bh.Options{
		Configure: func(c *bh.Client) {
			c.RegisterText("client/identify", func(question string) string {
				return "go-sdk:" + question
			})
		},
	})

	got, err := client.CallText(context.Background(), "callback", "hello")
	if err != nil {
		t.Fatalf("call: %v", err)
	}
	if got != "go-sdk:hello" {
		t.Errorf("got %q", got)
	}
}

func TestInteropPublishReachesASubscriber(t *testing.T) {
	port := interopServer(t)
	address := fmt.Sprintf("127.0.0.1:%d", port)

	received := make(chan string, 4)
	subscriber, err := bh.Connect(context.Background(), address, nil)
	if err != nil {
		t.Fatalf("connect subscriber: %v", err)
	}
	defer subscriber.Close()

	if err := subscriber.Subscribe("sensor/+/temperature", func(topic string, payload []byte) {
		received <- topic + "=" + string(payload)
	}); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	time.Sleep(300 * time.Millisecond)

	publisher, err := bh.Connect(context.Background(), address, nil)
	if err != nil {
		t.Fatalf("connect publisher: %v", err)
	}
	defer publisher.Close()

	if err := publisher.PublishText("sensor/tank-3/temperature", "28.4"); err != nil {
		t.Fatalf("publish: %v", err)
	}
	// This one matches no filter and must not arrive.
	if err := publisher.PublishText("sensor/tank-3/humidity", "62"); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case got := <-received:
		if got != "sensor/tank-3/temperature=28.4" {
			t.Errorf("got %q", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no delivery within 5s")
	}

	select {
	case got := <-received:
		t.Errorf("a non-matching topic was delivered: %q", got)
	case <-time.After(500 * time.Millisecond):
	}
}

func TestInteropMultiSegmentWildcard(t *testing.T) {
	port := interopServer(t)
	address := fmt.Sprintf("127.0.0.1:%d", port)

	received := make(chan string, 8)
	subscriber, err := bh.Connect(context.Background(), address, nil)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer subscriber.Close()

	_ = subscriber.Subscribe("alarm/#", func(topic string, _ []byte) { received <- topic })
	time.Sleep(300 * time.Millisecond)

	publisher, err := bh.Connect(context.Background(), address, nil)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer publisher.Close()

	_ = publisher.PublishText("alarm/floor-1/pump", "overheating")
	_ = publisher.PublishText("alarm/floor-2/valve/inlet", "stuck")

	seen := map[string]bool{}
	deadline := time.After(5 * time.Second)
	for len(seen) < 2 {
		select {
		case topic := <-received:
			seen[topic] = true
		case <-deadline:
			t.Fatalf("only saw %v", seen)
		}
	}
}

func TestInteropStreamArrivesComplete(t *testing.T) {
	port := interopServer(t)
	client, err := bh.Connect(context.Background(), fmt.Sprintf("127.0.0.1:%d", port), nil)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Close()

	confirmed := make(chan string, 1)
	_ = client.Subscribe("stream/done", func(_ string, payload []byte) {
		select {
		case confirmed <- string(payload):
		default:
		}
	})
	time.Sleep(300 * time.Millisecond)

	payload := make([]byte, 512*1024)
	for i := range payload {
		payload[i] = byte((i * 7) % 256)
	}

	sent, err := client.SendStream(
		context.Background(),
		"firmware-2026",
		strings.NewReader(string(payload)),
		bh.StreamDescriptor{Name: "firmware.bin", TotalLength: int64(len(payload)), ContentType: "application/octet-stream"},
		16*1024,
		nil,
	)
	if err != nil {
		t.Fatalf("send stream: %v", err)
	}
	if sent != int64(len(payload)) {
		t.Errorf("sent %d bytes, want %d", sent, len(payload))
	}

	select {
	case got := <-confirmed:
		want := fmt.Sprintf("firmware-2026:%d", len(payload))
		if got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the server never confirmed the stream")
	}
}

func TestInteropBatchedMessagesAreRoutedIndividually(t *testing.T) {
	port := interopServer(t)
	address := fmt.Sprintf("127.0.0.1:%d", port)

	const count = 300
	var seen atomic.Int64
	done := make(chan struct{})
	var once sync.Once

	subscriber, err := bh.Connect(context.Background(), address, nil)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer subscriber.Close()

	_ = subscriber.Subscribe("log/#", func(_ string, _ []byte) {
		if seen.Add(1) == count {
			once.Do(func() { close(done) })
		}
	})
	time.Sleep(300 * time.Millisecond)

	publisher, err := bh.Connect(context.Background(), address, nil)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer publisher.Close()

	messages := make([]bh.Message, count)
	for i := range messages {
		messages[i] = bh.Message{
			Type:    bh.TypePublish,
			Header:  fmt.Sprintf("log/entry/%d", i),
			Payload: []byte(fmt.Sprintf("line %d", i)),
		}
	}
	if err := publisher.SendBatch(messages); err != nil {
		t.Fatalf("send batch: %v", err)
	}

	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatalf("only %d of %d batched messages arrived", seen.Load(), count)
	}
}

func TestInteropKeepaliveRoundTripIsMeasured(t *testing.T) {
	client := connect(t, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// A single probe can legitimately read 0: time.Now on Windows resolves to about 500us, which
	// is coarser than a loopback round trip. Averaging over many probes gives a figure the clock
	// can actually represent, so this stays meaningful instead of flaky.
	if _, err := client.Ping(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}

	average, err := client.PingAverage(ctx, 50)
	if err != nil {
		t.Fatalf("ping average: %v", err)
	}
	if average <= 0 || average > 5*time.Second {
		t.Errorf("implausible average round trip: %v", average)
	}
	if got := client.Stats.LastRoundTrip(); got != average {
		t.Errorf("statistics report %v, want %v", got, average)
	}
}

func TestInteropStatisticsCountBothDirections(t *testing.T) {
	client := connect(t, nil)

	for i := 0; i < 25; i++ {
		if _, err := client.CallText(context.Background(), "upper", "abc"); err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
	}

	if got := client.Stats.MessagesSent(); got < 25 {
		t.Errorf("messages sent: %d", got)
	}
	if got := client.Stats.MessagesReceived(); got < 25 {
		t.Errorf("messages received: %d", got)
	}
	if client.Stats.BytesSent() == 0 {
		t.Error("bytes sent is zero")
	}
}

func TestInteropPendingCallsFailWhenTheConnectionCloses(t *testing.T) {
	port := interopServer(t)
	client, err := bh.Connect(context.Background(), fmt.Sprintf("127.0.0.1:%d", port), nil)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}

	result := make(chan error, 1)
	go func() {
		_, callErr := client.CallText(context.Background(), "sleep", "30000")
		result <- callErr
	}()

	time.Sleep(300 * time.Millisecond)
	_ = client.Close()

	select {
	case err := <-result:
		if err == nil {
			t.Fatal("expected the pending call to fail")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the pending call never completed")
	}
}

// asRPCError is a tiny errors.As helper kept local so the test file needs no extra import.
func asRPCError(err error, target **bh.RPCError) bool {
	for err != nil {
		if rpcErr, ok := err.(*bh.RPCError); ok {
			*target = rpcErr
			return true
		}
		unwrapper, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = unwrapper.Unwrap()
	}
	return false
}
