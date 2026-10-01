// Package bigpipe is the Go SDK for BigPipe, the high performance realtime stream
// processing platform. It talks to the BigPipe HTTP gateway (port 8082) and admin API
// (port 9644) using only the standard library, so it cross-compiles without cgo.
//
// Dibuat oleh Gravicode Studios dipimpin oleh Kang Fadhil.
package bigpipe

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

// Acks is accepted for parity with Kafka-protocol clients; the gateway acknowledges after the broker append.
type Acks int

const (
	AcksNone   Acks = 0
	AcksLeader Acks = 1
	AcksAll    Acks = -1
)

// Compression codec applied by the broker to the record batch.
type Compression string

const (
	None   Compression = ""
	Gzip   Compression = "gzip"
	Snappy Compression = "snappy"
	Lz4    Compression = "lz4"
	Zstd   Compression = "zstd"
)

// Record is a record to produce or a consumed record.
type Record struct {
	Topic         string
	Partition     int32
	Offset        int64
	Timestamp     time.Time
	Key           []byte
	Value         []byte
	Headers       map[string]string
	DeliveryCount int
	// PartitionSet forces Partition on produce (otherwise key hash or round-robin).
	PartitionSet bool
}

// JSON unmarshals the value into v.
func (r *Record) JSON(v any) error { return json.Unmarshal(r.Value, v) }

// RecordMetadata says where a record was written.
type RecordMetadata struct {
	Topic     string
	Partition int32
	Offset    int64
}

// Error is returned for non-2xx responses.
type Error struct {
	Status  int
	Code    string
	Message string
}

func (e *Error) Error() string { return fmt.Sprintf("%d %s: %s", e.Status, e.Code, e.Message) }

// IsNotFound reports whether err is a 404 from BigPipe.
func IsNotFound(err error) bool {
	var e *Error
	return errors.As(err, &e) && e.Status == 404
}

func gatewayURL(explicit string, bootstrap []string) string {
	if explicit != "" {
		return strings.TrimRight(explicit, "/")
	}
	if len(bootstrap) > 0 {
		host := bootstrap[0]
		if i := strings.LastIndex(host, ":"); i > 0 {
			host = host[:i]
		}
		return "http://" + host + ":8082"
	}
	if v := os.Getenv("BIGPIPE_HTTP"); v != "" {
		return strings.TrimRight(v, "/")
	}
	return "http://localhost:8082"
}

type client struct {
	base   string
	http   *http.Client
	apiKey string
}

func (c *client) do(ctx context.Context, method, path string, query url.Values, body any, out any) error {
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rdr = bytes.NewReader(b)
	}
	u := c.base + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, method, u, rdr)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode >= 400 {
		var e struct {
			Error struct{ Code, Message string } `json:"error"`
		}
		_ = json.Unmarshal(data, &e)
		if e.Error.Code == "" {
			e.Error.Code, e.Error.Message = "error", string(data)
		}
		return &Error{Status: resp.StatusCode, Code: e.Error.Code, Message: e.Error.Message}
	}
	if out != nil && len(data) > 0 {
		return json.Unmarshal(data, out)
	}
	return nil
}

type wireRecord struct {
	Topic         string             `json:"topic"`
	Partition     int32              `json:"partition"`
	Offset        int64              `json:"offset"`
	Timestamp     int64              `json:"timestamp"`
	Key           *string            `json:"key"`
	KeyEncoding   string             `json:"key_encoding"`
	Value         *string            `json:"value"`
	ValueEncoding string             `json:"value_encoding"`
	Headers       map[string]*string `json:"headers"`
	DeliveryCount int                `json:"delivery_count"`
}

func decodeField(v *string, enc string) []byte {
	if v == nil {
		return nil
	}
	if enc == "base64" {
		b, _ := base64.StdEncoding.DecodeString(*v)
		return b
	}
	return []byte(*v)
}

func (w wireRecord) toRecord() *Record {
	h := make(map[string]string, len(w.Headers))
	for k, v := range w.Headers {
		if v != nil {
			h[k] = *v
		}
	}
	return &Record{
		Topic:         w.Topic,
		Partition:     w.Partition,
		Offset:        w.Offset,
		Timestamp:     time.UnixMilli(w.Timestamp),
		Key:           decodeField(w.Key, w.KeyEncoding),
		Value:         decodeField(w.Value, w.ValueEncoding),
		Headers:       h,
		DeliveryCount: w.DeliveryCount,
	}
}

// ----------------------------------------------------------------------------------------------
// Producer
// ----------------------------------------------------------------------------------------------

// ProducerConfig configures a Producer.
type ProducerConfig struct {
	// Bootstrap is the Kafka bootstrap list; the gateway is assumed on the first host, port 8082.
	Bootstrap   []string
	URL         string // explicit gateway URL (overrides Bootstrap)
	Acks        Acks
	Compression Compression
	Linger      time.Duration // default 5ms
	MaxBatch    int           // default 500
}

type pendingSend struct {
	rec  map[string]any
	done chan sendResult
}

type sendResult struct {
	meta RecordMetadata
	err  error
}

// Producer batches records per topic and sends them to the gateway.
// It is safe for concurrent use.
type Producer struct {
	cfg     ProducerConfig
	c       *client
	mu      sync.Mutex
	pending map[string][]pendingSend
	wake    chan struct{}
	stop    chan struct{}
	stopped chan struct{}
	once    sync.Once
}

// NewProducer creates a producer and starts its background sender.
func NewProducer(cfg ProducerConfig) (*Producer, error) {
	if cfg.Linger == 0 {
		cfg.Linger = 5 * time.Millisecond
	}
	if cfg.MaxBatch <= 0 {
		cfg.MaxBatch = 500
	}
	p := &Producer{
		cfg:     cfg,
		c:       &client{base: gatewayURL(cfg.URL, cfg.Bootstrap), http: &http.Client{Timeout: 60 * time.Second}},
		pending: map[string][]pendingSend{},
		wake:    make(chan struct{}, 1),
		stop:    make(chan struct{}),
		stopped: make(chan struct{}),
	}
	go p.loop()
	return p, nil
}

func encodeRecord(r *Record) map[string]any {
	m := map[string]any{}
	if r.Key != nil {
		m["key"] = base64.StdEncoding.EncodeToString(r.Key)
		m["key_encoding"] = "base64"
	}
	if r.Value != nil {
		m["value"] = base64.StdEncoding.EncodeToString(r.Value)
		m["value_encoding"] = "base64"
	}
	if len(r.Headers) > 0 {
		m["headers"] = r.Headers
	}
	if r.PartitionSet {
		m["partition"] = r.Partition
	}
	if !r.Timestamp.IsZero() {
		m["timestamp"] = r.Timestamp.UnixMilli()
	}
	return m
}

// Send queues a record and waits until the broker has written it.
func (p *Producer) Send(ctx context.Context, r *Record) (RecordMetadata, error) {
	ps := pendingSend{rec: encodeRecord(r), done: make(chan sendResult, 1)}
	p.mu.Lock()
	p.pending[r.Topic] = append(p.pending[r.Topic], ps)
	full := len(p.pending[r.Topic]) >= p.cfg.MaxBatch
	p.mu.Unlock()
	if full {
		select {
		case p.wake <- struct{}{}:
		default:
		}
	}
	select {
	case res := <-ps.done:
		return res.meta, res.err
	case <-ctx.Done():
		return RecordMetadata{}, ctx.Err()
	}
}

func (p *Producer) loop() {
	defer close(p.stopped)
	t := time.NewTicker(p.cfg.Linger)
	defer t.Stop()
	for {
		select {
		case <-p.stop:
			p.flush()
			return
		case <-t.C:
		case <-p.wake:
		}
		p.flush()
	}
}

func (p *Producer) flush() {
	p.mu.Lock()
	batches := p.pending
	p.pending = map[string][]pendingSend{}
	p.mu.Unlock()
	var wg sync.WaitGroup
	for topic, items := range batches {
		for i := 0; i < len(items); i += p.cfg.MaxBatch {
			end := i + p.cfg.MaxBatch
			if end > len(items) {
				end = len(items)
			}
			wg.Add(1)
			go func(topic string, batch []pendingSend) {
				defer wg.Done()
				p.post(topic, batch)
			}(topic, items[i:end])
		}
	}
	wg.Wait()
}

func (p *Producer) post(topic string, batch []pendingSend) {
	recs := make([]map[string]any, len(batch))
	for i, b := range batch {
		recs[i] = b.rec
	}
	body := map[string]any{"records": recs}
	if p.cfg.Compression != None {
		body["compression"] = string(p.cfg.Compression)
	}
	var out struct {
		Offsets []struct {
			Partition int32 `json:"partition"`
			Offset    int64 `json:"offset"`
		} `json:"offsets"`
	}
	err := p.c.do(context.Background(), "POST", "/v1/topics/"+url.PathEscape(topic)+"/records", nil, body, &out)
	for i, b := range batch {
		if err != nil || i >= len(out.Offsets) {
			if err == nil {
				err = errors.New("bigpipe: short produce response")
			}
			b.done <- sendResult{err: err}
			continue
		}
		b.done <- sendResult{meta: RecordMetadata{Topic: topic, Partition: out.Offsets[i].Partition, Offset: out.Offsets[i].Offset}}
	}
}

// Close sends queued records and stops the producer.
func (p *Producer) Close() error {
	p.once.Do(func() { close(p.stop) })
	<-p.stopped
	return nil
}

// ----------------------------------------------------------------------------------------------
// Consumer
// ----------------------------------------------------------------------------------------------

// ConsumerConfig configures a group member.
type ConsumerConfig struct {
	Bootstrap       []string
	URL             string
	Group           string
	Topics          []string
	AutoOffsetReset string // "earliest" or "latest" (default "latest")
	// DisableAutoCommit turns off committing positions after every poll.
	DisableAutoCommit bool
	Filter            string // bpql, evaluated by the broker
	PollTimeout       time.Duration
	MaxRecords        int
}

// Consumer is a member of a server-assigned consumer group.
type Consumer struct {
	cfg      ConsumerConfig
	c        *client
	memberID string
	mu       sync.Mutex
}

// NewConsumer joins the group.
func NewConsumer(cfg ConsumerConfig) (*Consumer, error) {
	if cfg.Group == "" || len(cfg.Topics) == 0 {
		return nil, errors.New("bigpipe: Group and Topics are required")
	}
	if cfg.AutoOffsetReset == "" {
		cfg.AutoOffsetReset = "latest"
	}
	if cfg.PollTimeout == 0 {
		cfg.PollTimeout = time.Second
	}
	if cfg.MaxRecords == 0 {
		cfg.MaxRecords = 500
	}
	c := &Consumer{cfg: cfg, c: &client{base: gatewayURL(cfg.URL, cfg.Bootstrap), http: &http.Client{Timeout: cfg.PollTimeout + 30*time.Second}}}
	return c, c.join(context.Background())
}

func (c *Consumer) join(ctx context.Context) error {
	var out struct {
		MemberID string `json:"member_id"`
	}
	err := c.c.do(ctx, "POST", "/v1/groups/"+url.PathEscape(c.cfg.Group)+"/members", nil, map[string]any{
		"topics": c.cfg.Topics, "auto_commit": !c.cfg.DisableAutoCommit, "offset_reset": c.cfg.AutoOffsetReset, "filter": c.cfg.Filter,
	}, &out)
	if err == nil {
		c.mu.Lock()
		c.memberID = out.MemberID
		c.mu.Unlock()
	}
	return err
}

func (c *Consumer) member() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.memberID
}

// Poll returns the next records (possibly none after the poll timeout).
func (c *Consumer) Poll(ctx context.Context) ([]*Record, error) {
	q := url.Values{}
	q.Set("timeout_ms", fmt.Sprint(c.cfg.PollTimeout.Milliseconds()))
	q.Set("max_records", fmt.Sprint(c.cfg.MaxRecords))
	var out struct {
		Records []wireRecord `json:"records"`
	}
	err := c.c.do(ctx, "GET", "/v1/groups/"+url.PathEscape(c.cfg.Group)+"/members/"+c.member()+"/records", q, nil, &out)
	if IsNotFound(err) {
		return nil, c.join(ctx) // session expired: rejoin
	}
	if err != nil {
		return nil, err
	}
	recs := make([]*Record, len(out.Records))
	for i, w := range out.Records {
		recs[i] = w.toRecord()
	}
	return recs, nil
}

// Records streams records until ctx is cancelled; errors are retried after a short pause.
func (c *Consumer) Records(ctx context.Context) <-chan *Record {
	ch := make(chan *Record, c.cfg.MaxRecords)
	go func() {
		defer close(ch)
		for ctx.Err() == nil {
			recs, err := c.Poll(ctx)
			if err != nil {
				select {
				case <-ctx.Done():
					return
				case <-time.After(500 * time.Millisecond):
				}
				continue
			}
			for _, r := range recs {
				select {
				case ch <- r:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return ch
}

// Commit commits the position after rec.
func (c *Consumer) Commit(ctx context.Context, rec *Record) error {
	body := map[string]any{"offsets": []map[string]any{{"topic": rec.Topic, "partition": rec.Partition, "offset": rec.Offset + 1}}}
	return c.c.do(ctx, "POST", "/v1/groups/"+url.PathEscape(c.cfg.Group)+"/members/"+c.member()+"/commit", nil, body, nil)
}

// Close leaves the group.
func (c *Consumer) Close() error {
	return c.c.do(context.Background(), "DELETE", "/v1/groups/"+url.PathEscape(c.cfg.Group)+"/members/"+c.member(), nil, nil, nil)
}

// ----------------------------------------------------------------------------------------------
// Share groups
// ----------------------------------------------------------------------------------------------

// AckAction is the outcome of processing a share-group record.
type AckAction string

const (
	Accept  AckAction = "accept"
	Release AckAction = "release"
	Reject  AckAction = "reject"
)

// ShareConsumerConfig configures a share-group worker.
type ShareConsumerConfig struct {
	Bootstrap       []string
	URL             string
	Group           string
	Topics          []string
	Member          string
	MaxRecords      int
	Lock            time.Duration
	MaxAttempts     int
	DeadLetterTopic string
	Filter          string
	PollTimeout     time.Duration
}

// ShareConsumer consumes with queue semantics: per-record locks and acknowledgements.
type ShareConsumer struct {
	cfg ShareConsumerConfig
	c   *client
}

// NewShareConsumer creates a share-group worker.
func NewShareConsumer(cfg ShareConsumerConfig) (*ShareConsumer, error) {
	if cfg.Group == "" || len(cfg.Topics) == 0 {
		return nil, errors.New("bigpipe: Group and Topics are required")
	}
	if cfg.Member == "" {
		cfg.Member = fmt.Sprintf("go-%x", rand.Int63())
	}
	if cfg.MaxRecords == 0 {
		cfg.MaxRecords = 100
	}
	if cfg.Lock == 0 {
		cfg.Lock = 30 * time.Second
	}
	if cfg.MaxAttempts == 0 {
		cfg.MaxAttempts = 5
	}
	if cfg.PollTimeout == 0 {
		cfg.PollTimeout = time.Second
	}
	return &ShareConsumer{cfg: cfg, c: &client{base: gatewayURL(cfg.URL, cfg.Bootstrap), http: &http.Client{Timeout: cfg.PollTimeout + 30*time.Second}}}, nil
}

// Poll acquires up to MaxRecords records for this worker.
func (s *ShareConsumer) Poll(ctx context.Context) ([]*Record, error) {
	var out struct {
		Records []wireRecord `json:"records"`
	}
	err := s.c.do(ctx, "POST", "/v1/share/"+url.PathEscape(s.cfg.Group)+"/poll", nil, map[string]any{
		"member": s.cfg.Member, "topics": s.cfg.Topics, "max_records": s.cfg.MaxRecords, "lock_ms": s.cfg.Lock.Milliseconds(),
		"max_attempts": s.cfg.MaxAttempts, "dlq_topic": nilIfEmpty(s.cfg.DeadLetterTopic), "filter": nilIfEmpty(s.cfg.Filter),
		"timeout_ms": s.cfg.PollTimeout.Milliseconds(),
	}, &out)
	if err != nil {
		return nil, err
	}
	recs := make([]*Record, len(out.Records))
	for i, w := range out.Records {
		recs[i] = w.toRecord()
	}
	return recs, nil
}

// Ack acknowledges records with one action each (parallel slices).
func (s *ShareConsumer) Ack(ctx context.Context, recs []*Record, actions []AckAction) ([]bool, error) {
	acks := make([]map[string]any, len(recs))
	for i, r := range recs {
		acks[i] = map[string]any{"topic": r.Topic, "partition": r.Partition, "offset": r.Offset, "action": actions[i]}
	}
	var out struct {
		Results []bool `json:"results"`
	}
	err := s.c.do(ctx, "POST", "/v1/share/"+url.PathEscape(s.cfg.Group)+"/ack", nil, map[string]any{"member": s.cfg.Member, "acks": acks}, &out)
	return out.Results, err
}

func nilIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// ----------------------------------------------------------------------------------------------
// Streaming (SSE)
// ----------------------------------------------------------------------------------------------

// StreamOptions configures Stream.
type StreamOptions struct {
	Bootstrap []string
	URL       string
	From      string // "earliest", "latest" (default) or an offset
	Filter    string // bpql evaluated by the broker
	Group     string
}

// Stream delivers records over Server-Sent Events until ctx is cancelled.
// The error channel receives at most one error and is closed with the record channel.
func Stream(ctx context.Context, topic string, opts StreamOptions) (<-chan *Record, <-chan error) {
	out := make(chan *Record, 256)
	errc := make(chan error, 1)
	go func() {
		defer close(out)
		defer close(errc)
		q := url.Values{}
		from := opts.From
		if from == "" {
			from = "latest"
		}
		q.Set("from", from)
		if opts.Filter != "" {
			q.Set("filter", opts.Filter)
		}
		if opts.Group != "" {
			q.Set("group", opts.Group)
		}
		u := gatewayURL(opts.URL, opts.Bootstrap) + "/v1/topics/" + url.PathEscape(topic) + "/stream?" + q.Encode()
		req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
		if err != nil {
			errc <- err
			return
		}
		req.Header.Set("Accept", "text/event-stream")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			if ctx.Err() == nil {
				errc <- err
			}
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode >= 400 {
			b, _ := io.ReadAll(resp.Body)
			errc <- &Error{Status: resp.StatusCode, Code: "stream_failed", Message: string(b)}
			return
		}
		sc := bufio.NewScanner(resp.Body)
		sc.Buffer(make([]byte, 64*1024), 16*1024*1024)
		event := ""
		for sc.Scan() {
			line := sc.Text()
			switch {
			case strings.HasPrefix(line, "event:"):
				event = strings.TrimSpace(line[6:])
			case strings.HasPrefix(line, "data:") && event == "record":
				var w wireRecord
				if json.Unmarshal([]byte(line[5:]), &w) == nil {
					select {
					case out <- w.toRecord():
					case <-ctx.Done():
						return
					}
				}
			case line == "":
				event = ""
			}
		}
		if err := sc.Err(); err != nil && ctx.Err() == nil {
			errc <- err
		}
	}()
	return out, errc
}

// ----------------------------------------------------------------------------------------------
// Admin
// ----------------------------------------------------------------------------------------------

// Admin is a client for the BigPipe admin API.
type Admin struct{ c *client }

// NewAdmin creates an admin client ("" = BIGPIPE_ADMIN or http://localhost:9644).
func NewAdmin(adminURL, apiKey string) *Admin {
	if adminURL == "" {
		adminURL = os.Getenv("BIGPIPE_ADMIN")
	}
	if adminURL == "" {
		adminURL = "http://localhost:9644"
	}
	if apiKey == "" {
		apiKey = os.Getenv("BIGPIPE_API_KEY")
	}
	return &Admin{c: &client{base: strings.TrimRight(adminURL, "/"), http: &http.Client{Timeout: 60 * time.Second}, apiKey: apiKey}}
}

// TopicInfo is a summary of a topic.
type TopicInfo struct {
	Name       string            `json:"name"`
	Partitions int               `json:"partitions"`
	Mode       string            `json:"mode"`
	Records    int64             `json:"records"`
	Config     map[string]string `json:"config"`
	Bytes      struct {
		Local    int64 `json:"local"`
		Remote   int64 `json:"remote"`
		Diskless int64 `json:"diskless"`
	} `json:"bytes"`
}

// CreateTopic creates a topic with a storage mode: "local", "tiered" or "diskless".
func (a *Admin) CreateTopic(ctx context.Context, name string, partitions int, mode string, config map[string]string) (*TopicInfo, error) {
	var t TopicInfo
	if config == nil {
		config = map[string]string{}
	}
	err := a.c.do(ctx, "POST", "/v1/topics", nil, map[string]any{"name": name, "partitions": partitions, "mode": mode, "config": config}, &t)
	return &t, err
}

// Topics lists topics.
func (a *Admin) Topics(ctx context.Context) ([]TopicInfo, error) {
	var out []TopicInfo
	return out, a.c.do(ctx, "GET", "/v1/topics", nil, nil, &out)
}

// Topic returns one topic.
func (a *Admin) Topic(ctx context.Context, name string) (*TopicInfo, error) {
	var t TopicInfo
	return &t, a.c.do(ctx, "GET", "/v1/topics/"+url.PathEscape(name), nil, nil, &t)
}

// DeleteTopic deletes a topic.
func (a *Admin) DeleteTopic(ctx context.Context, name string) error {
	return a.c.do(ctx, "DELETE", "/v1/topics/"+url.PathEscape(name), nil, nil, nil)
}

// Migrate moves new data of a topic to another storage mode (online, offsets unchanged).
func (a *Admin) Migrate(ctx context.Context, name, to string) (map[string]any, error) {
	var out map[string]any
	return out, a.c.do(ctx, "POST", "/v1/topics/"+url.PathEscape(name)+"/migrate", nil, map[string]any{"to": to}, &out)
}

// Compact compacts a cleanup.policy=compact topic now, keeping the latest record per key.
func (a *Admin) Compact(ctx context.Context, name string) (map[string]any, error) {
	var out map[string]any
	return out, a.c.do(ctx, "POST", "/v1/topics/"+url.PathEscape(name)+"/compact", nil, map[string]any{}, &out)
}

// Group returns a group with per-partition lag.
func (a *Admin) Group(ctx context.Context, id string) (map[string]any, error) {
	var out map[string]any
	return out, a.c.do(ctx, "GET", "/v1/groups/"+url.PathEscape(id), nil, nil, &out)
}

// Browse returns recent records of a topic, optionally filtered with bpql.
func (a *Admin) Browse(ctx context.Context, topic string, limit int, filter string) ([]*Record, error) {
	q := url.Values{}
	q.Set("limit", fmt.Sprint(limit))
	if filter != "" {
		q.Set("filter", filter)
	}
	var out struct {
		Records []wireRecord `json:"records"`
	}
	if err := a.c.do(ctx, "GET", "/v1/topics/"+url.PathEscape(topic)+"/messages", q, nil, &out); err != nil {
		return nil, err
	}
	recs := make([]*Record, len(out.Records))
	for i, w := range out.Records {
		recs[i] = w.toRecord()
	}
	return recs, nil
}
