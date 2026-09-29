package bigpipe

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

// Integration tests against a live node (BIGPIPE_HTTP / BIGPIPE_ADMIN, default localhost).

func uid(p string) string { return fmt.Sprintf("%s-%08x", p, rand.Uint32()) }

func TestProduceConsumeGroup(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	admin := NewAdmin("", "")
	topic := uid("go-orders")
	if _, err := admin.CreateTopic(ctx, topic, 3, "local", nil); err != nil {
		t.Fatal(err)
	}
	p, _ := NewProducer(ProducerConfig{Bootstrap: []string{"localhost:9092"}, Compression: Zstd})
	var wg sync.WaitGroup
	errs := make(chan error, 200)
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := p.Send(ctx, &Record{Topic: topic, Key: []byte(fmt.Sprintf("k%d", i)), Value: []byte(fmt.Sprintf(`{"n":%d}`, i)), Headers: map[string]string{"lang": "go"}})
			if err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	p.Close()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	c, err := NewConsumer(ConsumerConfig{Group: uid("g"), Topics: []string{topic}, AutoOffsetReset: "earliest"})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	seen := map[int]bool{}
	var last *Record
	for rec := range c.Records(ctx) {
		var v struct{ N int }
		if err := rec.JSON(&v); err != nil {
			t.Fatal(err)
		}
		if rec.Headers["lang"] != "go" {
			t.Fatalf("missing header: %v", rec.Headers)
		}
		seen[v.N] = true
		last = rec
		if len(seen) == 200 {
			break
		}
	}
	if len(seen) != 200 {
		t.Fatalf("got %d records", len(seen))
	}
	if err := c.Commit(ctx, last); err != nil {
		t.Fatal(err)
	}
}

func TestShareConsumerAndStream(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	admin := NewAdmin("", "")
	topic := uid("go-jobs")
	dlq := topic + ".dlq"
	if _, err := admin.CreateTopic(ctx, topic, 1, "diskless", nil); err != nil {
		t.Fatal(err)
	}
	stream, errc := Stream(ctx, topic, StreamOptions{From: "earliest", Filter: `value() startsWith "job-1"`})
	p, _ := NewProducer(ProducerConfig{})
	for i := 0; i < 3; i++ {
		if _, err := p.Send(ctx, &Record{Topic: topic, Value: []byte(fmt.Sprintf("job-%d", i))}); err != nil {
			t.Fatal(err)
		}
	}
	p.Close()
	select {
	case r := <-stream:
		if string(r.Value) != "job-1" {
			t.Fatalf("filter let through %q", r.Value)
		}
	case err := <-errc:
		t.Fatal(err)
	case <-ctx.Done():
		t.Fatal("no streamed record")
	}

	w, _ := NewShareConsumer(ShareConsumerConfig{Group: uid("w"), Topics: []string{topic}, DeadLetterTopic: dlq})
	recs, err := w.Poll(ctx)
	if err != nil || len(recs) != 3 {
		t.Fatalf("poll: %v (%d records)", err, len(recs))
	}
	actions := make([]AckAction, len(recs))
	for i, r := range recs {
		actions[i] = Accept
		if string(r.Value) == "job-2" {
			actions[i] = Reject
		}
	}
	if res, err := w.Ack(ctx, recs, actions); err != nil || len(res) != 3 {
		t.Fatalf("ack: %v %v", err, res)
	}
	var dead []*Record
	for i := 0; i < 20 && len(dead) == 0; i++ {
		dead, _ = admin.Browse(ctx, dlq, 10, "")
		time.Sleep(100 * time.Millisecond)
	}
	if len(dead) != 1 || string(dead[0].Value) != "job-2" {
		t.Fatalf("dlq: %v", dead)
	}
	if _, err := admin.Migrate(ctx, topic, "local"); err != nil {
		t.Fatal(err)
	}
}
