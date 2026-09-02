// Command benchmark compares transports from the Go client.
//
// BlackHole Messaging - Gravicode Studios, led by Kang Fadhil.
//
// It starts the .NET interop server once per transport and measures the same workload over each, so
// the numbers differ only in how the bytes travel:
//
//	go run ./example/benchmark
//
// Needs a .NET 10 SDK, or a prebuilt tests/BlackHole.InteropServer.
package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	bh "github.com/DotNetVibeCoderz/Vibe_Messaging/BlackHole/clients/go/v3/blackhole"
)

const (
	calls     = 5_000
	warmup    = 500
	publishes = 50_000
)

func main() {
	fmt.Println("==========================================================")
	fmt.Println("  BLACKHOLE MESSAGING - GO TRANSPORT COMPARISON")
	fmt.Println("==========================================================")
	fmt.Printf("  go           : %s\n", runtime.Version())
	fmt.Printf("  platform     : %s/%s\n", runtime.GOOS, runtime.GOARCH)
	fmt.Printf("  measured     : %s\n", time.Now().Format("2006-01-02 15:04"))
	fmt.Printf("  workload     : %d RPC calls, %d publishes\n", calls, publishes)
	fmt.Println("  note         : latency percentiles are per 50-call group; time.Now on Windows")
	fmt.Println("                 resolves to ~500us, coarser than a single round trip")
	fmt.Println()
	fmt.Println("  transport            p50       p90       p99      one-by-one     batched(256)")
	fmt.Println("  --------------   --------  --------  --------   -------------  -------------")

	ctx := context.Background()

	run(ctx, "TCP loopback", []string{"--port", "0"}, func(endpoint string) (*bh.Client, error) {
		return bh.Connect(ctx, "127.0.0.1:"+endpoint, nil)
	})

	// Go dials Unix domain sockets on Linux, macOS, and Windows 10 build 17063 or later.
	socketPath := filepath.Join(os.TempDir(), fmt.Sprintf("bh-bench-%d.sock", os.Getpid()))
	run(ctx, "Unix socket", []string{"--unix", socketPath}, func(string) (*bh.Client, error) {
		return bh.ConnectUnix(ctx, socketPath, nil)
	})

	fmt.Println()
	fmt.Println("  Named pipes need a third-party package on Windows, and shared memory needs a")
	fmt.Println("  mapped segment plus a dedicated polling thread - neither is something this SDK")
	fmt.Println("  offers. Both are available from .NET; see docs/transports.md.")
	fmt.Println()
	fmt.Println("Gravicode Studios - led by Kang Fadhil")
}

func run(ctx context.Context, label string, serverArgs []string, connect func(string) (*bh.Client, error)) {
	server, endpoint, err := startServer(serverArgs)
	if err != nil {
		fmt.Printf("  %-14s unavailable: %v\n", label, err)
		return
	}
	defer func() {
		_ = server.Process.Kill()
		_ = server.Wait()
	}()

	client, err := connect(endpoint)
	if err != nil {
		fmt.Printf("  %-14s unavailable: %v\n", label, err)
		return
	}
	defer client.Close()

	p50, p90, p99, err := measureLatency(ctx, client)
	if err != nil {
		fmt.Printf("  %-14s failed: %v\n", label, err)
		return
	}

	individual, batched, err := measureThroughput(ctx, client)
	if err != nil {
		fmt.Printf("  %-14s failed: %v\n", label, err)
		return
	}

	fmt.Printf("  %-14s %8.1fus %8.1fus %8.1fus   %11s/s %11s/s\n",
		label, p50, p90, p99, thousands(individual), thousands(batched))
}

// measureLatency times sequential RPC round trips, in microseconds per call.
//
// Calls are timed in groups rather than individually. time.Now on Windows resolves to roughly
// 500us, which is coarser than a single loopback round trip, so timing one call at a time reports
// a p50 of exactly zero. A group of 50 spans far more than one tick, and dividing gives a figure
// the clock can actually represent - at the cost of reporting per-group percentiles rather than
// per-call ones, which is the honest trade here.
func measureLatency(ctx context.Context, client *bh.Client) (float64, float64, float64, error) {
	for i := 0; i < warmup; i++ {
		if _, err := client.CallText(ctx, "upper", "x"); err != nil {
			return 0, 0, 0, err
		}
	}

	const perGroup = 50
	groups := calls / perGroup
	samples := make([]float64, groups)

	for g := 0; g < groups; g++ {
		started := time.Now()
		for i := 0; i < perGroup; i++ {
			if _, err := client.CallText(ctx, "upper", "x"); err != nil {
				return 0, 0, 0, err
			}
		}
		samples[g] = float64(time.Since(started).Nanoseconds()) / 1000 / perGroup
	}
	sort.Float64s(samples)

	at := func(q float64) float64 {
		index := int(q*float64(len(samples))) - 1
		if index < 0 {
			index = 0
		}
		return samples[index]
	}
	return at(0.50), at(0.90), at(0.99), nil
}

// measureThroughput publishes one at a time, then batched, to show what batching is worth here.
func measureThroughput(ctx context.Context, client *bh.Client) (float64, float64, error) {
	payload := []byte("28.4")

	started := time.Now()
	for i := 0; i < publishes; i++ {
		if err := client.Publish("t", payload); err != nil {
			return 0, 0, err
		}
	}
	individual := time.Since(started).Seconds()

	batch := make([]bh.Message, 256)
	for i := range batch {
		batch[i] = bh.Message{Type: bh.TypePublish, Header: "t", Payload: payload}
	}

	started = time.Now()
	for sent := 0; sent < publishes; sent += len(batch) {
		if err := client.SendBatch(batch); err != nil {
			return 0, 0, err
		}
	}
	batched := time.Since(started).Seconds()

	return float64(publishes) / individual, float64(publishes) / batched, nil
}

// startServer launches the interop server on one transport and waits for its READY line.
func startServer(args []string) (*exec.Cmd, string, error) {
	repoRoot, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		return nil, "", err
	}
	// Tolerate being run from either the module root or the example directory.
	if _, statErr := os.Stat(filepath.Join(repoRoot, "tests", "BlackHole.InteropServer")); statErr != nil {
		if repoRoot, err = filepath.Abs(filepath.Join("..", "..")); err != nil {
			return nil, "", err
		}
	}
	project := filepath.Join(repoRoot, "tests", "BlackHole.InteropServer")

	var command *exec.Cmd
	built := filepath.Join(project, "bin", "Release", "net10.0", "BlackHole.InteropServer.exe")
	if _, statErr := os.Stat(built); statErr == nil {
		command = exec.Command(built, args...)
	} else {
		command = exec.Command("dotnet",
			append([]string{"run", "--project", project, "-c", "Release", "--"}, args...)...)
	}
	command.Dir = repoRoot

	stdout, err := command.StdoutPipe()
	if err != nil {
		return nil, "", err
	}
	if err := command.Start(); err != nil {
		return nil, "", err
	}

	ready := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			if line := scanner.Text(); strings.HasPrefix(line, "READY ") {
				ready <- strings.TrimSpace(strings.TrimPrefix(line, "READY "))
				return
			}
		}
	}()

	select {
	case endpoint := <-ready:
		return command, endpoint, nil
	case <-time.After(120 * time.Second):
		_ = command.Process.Kill()
		return nil, "", fmt.Errorf("the server did not report an endpoint within 120s")
	}
}

// thousands formats a rate with separators, since these numbers are read by eye.
func thousands(value float64) string {
	digits := strconv.FormatFloat(value, 'f', 0, 64)
	var out []byte
	for i, c := range []byte(digits) {
		if i > 0 && (len(digits)-i)%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, c)
	}
	return string(out)
}
