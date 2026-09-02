// BlackHole Messaging - Gravicode Studios, led by Kang Fadhil.

package blackhole

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Handler receives one message. It runs on the client's read goroutine, in order, so it must not
// block: hand slow work to a goroutine or a channel.
type Handler func(Message)

// MethodHandler serves an RPC call the peer made on this client. Returning an error sends the peer
// an error response rather than a result.
type MethodHandler func(ctx context.Context, request Message) ([]byte, error)

// StreamHandler receives a completed inbound stream. The data slice is owned by the callback.
type StreamHandler func(streamID string, descriptor StreamDescriptor, data []byte)

// Options configures a client. The zero value is usable; Connect fills in sensible defaults.
type Options struct {
	// DialTimeout bounds establishing the TCP connection. Default 10s.
	DialTimeout time.Duration
	// CallTimeout is the deadline applied to a Call that does not carry its own. Default 30s.
	CallTimeout time.Duration
	// MaxFrameLength rejects any larger frame while parsing. Default 16 MiB.
	MaxFrameLength int
	// ReadBufferSize is the size of a single socket read. Default 64 KiB.
	ReadBufferSize int
	// FlushThreshold is how many bytes of stream chunks accumulate before a write. Default 64 KiB.
	FlushThreshold int
	// OnError, when set, receives the failure that ended the read loop.
	OnError func(error)
	// Configure runs after the client is built but before the read loop starts, so handlers
	// registered there cannot miss a message the server pushes the instant it accepts.
	Configure func(*Client)
}

func (o *Options) withDefaults() {
	if o.DialTimeout <= 0 {
		o.DialTimeout = 10 * time.Second
	}
	if o.CallTimeout <= 0 {
		o.CallTimeout = 30 * time.Second
	}
	if o.MaxFrameLength <= 0 {
		o.MaxFrameLength = DefaultMaxFrameLength
	}
	if o.ReadBufferSize <= 0 {
		o.ReadBufferSize = 64 * 1024
	}
	if o.FlushThreshold <= 0 {
		o.FlushThreshold = 64 * 1024
	}
}

// Statistics holds per-connection counters. Read them with the accessor methods; the fields are
// updated atomically from the read and write paths.
type Statistics struct {
	messagesSent     atomic.Int64
	messagesReceived atomic.Int64
	bytesSent        atomic.Int64
	bytesReceived    atomic.Int64
	lastRoundTrip    atomic.Int64 // nanoseconds, -1 when none has completed
}

// MessagesSent returns how many messages this connection has sent.
func (s *Statistics) MessagesSent() int64 { return s.messagesSent.Load() }

// MessagesReceived returns how many messages this connection has received.
func (s *Statistics) MessagesReceived() int64 { return s.messagesReceived.Load() }

// BytesSent returns how many bytes this connection has written.
func (s *Statistics) BytesSent() int64 { return s.bytesSent.Load() }

// BytesReceived returns how many bytes this connection has read.
func (s *Statistics) BytesReceived() int64 { return s.bytesReceived.Load() }

// LastRoundTrip returns the most recent keepalive round trip, or zero if none has completed.
func (s *Statistics) LastRoundTrip() time.Duration {
	nanos := s.lastRoundTrip.Load()
	if nanos < 0 {
		return 0
	}
	return time.Duration(nanos)
}

func (s *Statistics) String() string {
	return fmt.Sprintf("sent %d msg / %d B, received %d msg / %d B",
		s.MessagesSent(), s.BytesSent(), s.MessagesReceived(), s.BytesReceived())
}

type pendingCall struct {
	method string
	result chan callResult
}

type callResult struct {
	payload []byte
	err     error
}

type reassembly struct {
	descriptor StreamDescriptor
	buffer     []byte
	nextChunk  int64
}

type subscription struct {
	filter  string
	handler func(topic string, payload []byte)
}

// Client is a connected BlackHole client. It is safe for concurrent use.
type Client struct {
	conn    net.Conn
	options Options

	// Stats holds live counters for this connection.
	Stats *Statistics

	writeMu sync.Mutex

	mu            sync.RWMutex
	pending       map[int64]*pendingCall
	methods       map[string]MethodHandler
	subscriptions []subscription
	handlers      map[MessageType][]Handler
	streams       map[string]*reassembly
	streamHandler []StreamHandler

	correlation atomic.Int64

	// pong carries the answer to a probe back to Ping, which measures the elapsed time itself.
	// Storing a start timestamp and subtracting later would lose the monotonic reading that
	// time.Time carries, and on Windows the wall clock is too coarse to see a loopback round trip.
	pong chan struct{}

	closeOnce sync.Once
	closed    chan struct{}
	closeErr  atomic.Pointer[error]
}

// Connect dials host:port and starts receiving.
func Connect(ctx context.Context, address string, options *Options) (*Client, error) {
	opts := Options{}
	if options != nil {
		opts = *options
	}
	opts.withDefaults()

	dialer := net.Dialer{Timeout: opts.DialTimeout}
	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, fmt.Errorf("blackhole: dial %s: %w", address, err)
	}

	if tcp, ok := conn.(*net.TCPConn); ok {
		// BlackHole coalesces at the application layer, so letting the kernel hold small frames
		// only adds latency.
		_ = tcp.SetNoDelay(true)
	}

	client := &Client{
		conn:     conn,
		options:  opts,
		Stats:    &Statistics{},
		pending:  make(map[int64]*pendingCall),
		methods:  make(map[string]MethodHandler),
		handlers: make(map[MessageType][]Handler),
		streams:  make(map[string]*reassembly),
		closed:   make(chan struct{}),
		pong:     make(chan struct{}, 1),
	}
	client.Stats.lastRoundTrip.Store(-1)

	if opts.Configure != nil {
		opts.Configure(client)
	}

	go client.readLoop()
	return client, nil
}

// ConnectWithRetry dials with exponential backoff, for a client that may start before its server.
func ConnectWithRetry(ctx context.Context, address string, attempts int, initialDelay time.Duration, options *Options) (*Client, error) {
	if attempts <= 0 {
		attempts = 5
	}
	if initialDelay <= 0 {
		initialDelay = 100 * time.Millisecond
	}

	delay := initialDelay
	var last error
	for attempt := 1; attempt <= attempts; attempt++ {
		client, err := Connect(ctx, address, options)
		if err == nil {
			return client, nil
		}
		last = err
		if attempt == attempts {
			break
		}
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		if delay *= 2; delay > 5*time.Second {
			delay = 5 * time.Second
		}
	}
	return nil, fmt.Errorf("blackhole: could not connect to %s after %d attempts: %w", address, attempts, last)
}

// ---------------------------------------------------------------------- send

// Send writes one message to the peer.
func (c *Client) Send(m Message) error {
	frame, err := EncodeFrame(m)
	if err != nil {
		return err
	}
	return c.writeFrames(frame, 1)
}

// SendMany writes several messages in one socket write. Unlike SendBatch the peer sees each
// message individually framed; this only saves syscalls on the sending side.
func (c *Client) SendMany(messages []Message) error {
	if len(messages) == 0 {
		return nil
	}

	var buffer []byte
	for _, m := range messages {
		frame, err := EncodeFrame(m)
		if err != nil {
			return err
		}
		buffer = append(buffer, frame...)
	}
	return c.writeFrames(buffer, len(messages))
}

func (c *Client) writeFrames(frames []byte, count int) error {
	select {
	case <-c.closed:
		return errors.New("blackhole: the connection is closed")
	default:
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if _, err := c.conn.Write(frames); err != nil {
		return fmt.Errorf("blackhole: write: %w", err)
	}

	c.Stats.messagesSent.Add(int64(count))
	c.Stats.bytesSent.Add(int64(len(frames)))
	return nil
}

// ----------------------------------------------------------------------- RPC

// Call invokes a remote method and waits for its reply. The context bounds the wait alongside
// Options.CallTimeout, whichever expires first.
func (c *Client) Call(ctx context.Context, method string, payload []byte) ([]byte, error) {
	correlationID := c.correlation.Add(1)
	call := &pendingCall{method: method, result: make(chan callResult, 1)}

	c.mu.Lock()
	c.pending[correlationID] = call
	c.mu.Unlock()

	cleanup := func() {
		c.mu.Lock()
		delete(c.pending, correlationID)
		c.mu.Unlock()
	}

	if err := c.Send(Message{
		Type:          TypeRPCRequest,
		Header:        method,
		Payload:       payload,
		CorrelationID: correlationID,
	}); err != nil {
		cleanup()
		return nil, err
	}

	timer := time.NewTimer(c.options.CallTimeout)
	defer timer.Stop()

	select {
	case result := <-call.result:
		return result.payload, result.err
	case <-timer.C:
		cleanup()
		return nil, &RPCError{Method: method, Reason: "the call did not complete before its deadline"}
	case <-ctx.Done():
		cleanup()
		return nil, &RPCError{Method: method, Reason: ctx.Err().Error()}
	case <-c.closed:
		cleanup()
		return nil, &RPCError{Method: method, Reason: "the connection closed before the reply arrived"}
	}
}

// CallText is a text-in, text-out convenience wrapper around Call.
func (c *Client) CallText(ctx context.Context, method, payload string) (string, error) {
	result, err := c.Call(ctx, method, []byte(payload))
	if err != nil {
		return "", err
	}
	return string(result), nil
}

// Notify sends a request and never waits for a reply.
func (c *Client) Notify(method string, payload []byte) error {
	return c.Send(Message{
		Type:    TypeRPCRequest,
		Header:  method,
		Payload: payload,
		Flags:   FlagNoReply,
	})
}

// Register serves a method the peer may call on this client.
func (c *Client) Register(method string, handler MethodHandler) *Client {
	c.mu.Lock()
	c.methods[method] = handler
	c.mu.Unlock()
	return c
}

// RegisterText registers a text-in, text-out handler.
func (c *Client) RegisterText(method string, handler func(string) string) *Client {
	return c.Register(method, func(_ context.Context, request Message) ([]byte, error) {
		return []byte(handler(request.Text())), nil
	})
}

// ------------------------------------------------------------------ Pub/Sub

// Subscribe asks the broker for a topic or wildcard filter. When handler is non-nil it fires only
// for topics matching this filter; use OnPublish to see everything.
func (c *Client) Subscribe(filter string, handler func(topic string, payload []byte)) error {
	if handler != nil {
		c.mu.Lock()
		c.subscriptions = append(c.subscriptions, subscription{filter: filter, handler: handler})
		c.mu.Unlock()
	}
	return c.Send(Message{Type: TypeSubscribe, Header: filter})
}

// Unsubscribe stops receiving a filter.
func (c *Client) Unsubscribe(filter string) error {
	c.mu.Lock()
	kept := c.subscriptions[:0]
	for _, s := range c.subscriptions {
		if s.filter != filter {
			kept = append(kept, s)
		}
	}
	c.subscriptions = kept
	c.mu.Unlock()

	return c.Send(Message{Type: TypeUnsubscribe, Header: filter})
}

// Publish sends a payload to a topic.
func (c *Client) Publish(topic string, payload []byte) error {
	return c.Send(Message{Type: TypePublish, Header: topic, Payload: payload})
}

// PublishText sends UTF-8 text to a topic.
func (c *Client) PublishText(topic, payload string) error {
	return c.Publish(topic, []byte(payload))
}

// OnPublish receives every delivered message, whatever its topic.
func (c *Client) OnPublish(handler func(topic string, payload []byte)) *Client {
	return c.On(TypePublish, func(m Message) { handler(m.Header, m.Payload) })
}

// --------------------------------------------------------------- streaming

// SendStream sends a large body as chunks and returns the bytes sent.
//
// Chunks are accumulated and written once per Options.FlushThreshold rather than once per chunk,
// which is what keeps small chunk sizes fast.
func (c *Client) SendStream(ctx context.Context, streamID string, source io.Reader, descriptor StreamDescriptor, chunkSize int, progress func(int64)) (int64, error) {
	if chunkSize < 64 {
		chunkSize = 16 * 1024
	}
	if descriptor.ContentType == "" {
		descriptor.ContentType = "application/octet-stream"
	}
	if descriptor.Name == "" {
		descriptor.Name = streamID
	}

	if err := c.Send(Message{Type: TypeStreamStart, Header: streamID, Payload: descriptor.Encode()}); err != nil {
		return 0, err
	}

	buffer := make([]byte, chunkSize)
	var pending []byte
	var pendingCount int
	var sent, index int64

	abort := func(cause error) (int64, error) {
		_ = c.Send(Message{
			Type:    TypeStreamAbort,
			Header:  streamID,
			Payload: []byte(cause.Error()),
			Flags:   FlagError,
		})
		return sent, cause
	}

	for {
		select {
		case <-ctx.Done():
			return abort(ctx.Err())
		default:
		}

		read, err := source.Read(buffer)
		if read > 0 {
			frame, encodeErr := EncodeFrame(Message{
				Type:          TypeStreamChunk,
				Header:        streamID,
				Payload:       buffer[:read],
				CorrelationID: index,
			})
			if encodeErr != nil {
				return abort(encodeErr)
			}
			pending = append(pending, frame...)
			index++
			sent += int64(read)

			pendingCount++
			if len(pending) >= c.options.FlushThreshold {
				if writeErr := c.writeFrames(pending, pendingCount); writeErr != nil {
					return abort(writeErr)
				}
				pending = pending[:0]
				pendingCount = 0
				if progress != nil {
					progress(sent)
				}
			}
		}

		if err == io.EOF {
			break
		}
		if err != nil {
			return abort(err)
		}
	}

	if len(pending) > 0 {
		if err := c.writeFrames(pending, pendingCount); err != nil {
			return abort(err)
		}
	}

	if err := c.Send(Message{Type: TypeStreamEnd, Header: streamID, CorrelationID: index}); err != nil {
		return sent, err
	}
	if progress != nil {
		progress(sent)
	}
	return sent, nil
}

// OnStream receives completed inbound streams.
func (c *Client) OnStream(handler StreamHandler) *Client {
	c.mu.Lock()
	c.streamHandler = append(c.streamHandler, handler)
	c.mu.Unlock()
	return c
}

// ---------------------------------------------------------------- batching

// SendBatch packs several messages into one frame and one socket write.
//
// The envelope payload is a run of complete BlackHole frames, which is exactly what the peer's own
// codec unpacks - there is no second wire format.
func (c *Client) SendBatch(messages []Message) error {
	if len(messages) == 0 {
		return nil
	}

	var payload []byte
	for _, m := range messages {
		frame, err := EncodeFrame(m)
		if err != nil {
			return err
		}
		payload = append(payload, frame...)
	}

	return c.Send(Message{Type: TypeBatch, Payload: payload, CorrelationID: int64(len(messages))})
}

// ----------------------------------------------------------------- routing

// On registers a handler for one message type. Several handlers run in registration order.
func (c *Client) On(messageType MessageType, handler Handler) *Client {
	c.mu.Lock()
	c.handlers[messageType] = append(c.handlers[messageType], handler)
	c.mu.Unlock()
	return c
}

// --------------------------------------------------------------- read loop

func (c *Client) readLoop() {
	buffer := make([]byte, 0, c.options.ReadBufferSize)
	chunk := make([]byte, c.options.ReadBufferSize)

	var failure error
	for {
		read, err := c.conn.Read(chunk)
		if read > 0 {
			buffer = append(buffer, chunk[:read]...)

			offset := 0
			for {
				message, consumed, decodeErr := DecodeFrame(buffer[offset:], c.options.MaxFrameLength)
				if decodeErr != nil {
					failure = decodeErr
					break
				}
				if consumed == 0 {
					break
				}
				offset += consumed
				c.Stats.messagesReceived.Add(1)
				c.Stats.bytesReceived.Add(int64(consumed))
				c.dispatch(message)
			}
			if failure != nil {
				break
			}

			// Everything before offset is parsed; keep only the partial frame at the end.
			if offset > 0 {
				buffer = append(buffer[:0], buffer[offset:]...)
			}
		}

		if err != nil {
			if !errors.Is(err, io.EOF) {
				failure = err
			}
			break
		}
	}

	c.closeWith(failure)
}

func (c *Client) dispatch(m Message) {
	switch m.Type {
	case TypePing:
		// Answered here so keepalive never reaches application code.
		_ = c.Send(Message{Type: TypePong, CorrelationID: m.CorrelationID})
		return

	case TypePong:
		select {
		case c.pong <- struct{}{}:
		default: // Nobody is waiting on a probe; an unsolicited Pong is harmless.
		}
		return

	case TypeRPCResponse:
		c.completeCall(m)
		return

	case TypeRPCRequest:
		c.serveCall(m)
		return

	case TypeBatch:
		c.unpackBatch(m)
		return

	case TypeStreamStart, TypeStreamChunk, TypeStreamEnd, TypeStreamAbort:
		c.handleStream(m)

	case TypePublish:
		c.deliverPublish(m)
	}

	c.mu.RLock()
	handlers := append([]Handler(nil), c.handlers[m.Type]...)
	c.mu.RUnlock()

	for _, handler := range handlers {
		handler(m)
	}
}

func (c *Client) completeCall(m Message) {
	c.mu.Lock()
	call, found := c.pending[m.CorrelationID]
	delete(c.pending, m.CorrelationID)
	c.mu.Unlock()

	if !found {
		return // Late reply for a call that already timed out.
	}

	if m.IsError() {
		call.result <- callResult{err: &RPCError{Method: call.method, Reason: m.Text()}}
		return
	}

	// The payload aliases the read buffer, which is reused as soon as this returns, so the waiting
	// caller must be handed a copy.
	payload := make([]byte, len(m.Payload))
	copy(payload, m.Payload)
	call.result <- callResult{payload: payload}
}

func (c *Client) serveCall(m Message) {
	c.mu.RLock()
	handler, found := c.methods[m.Header]
	c.mu.RUnlock()

	if !found {
		_ = c.Send(Message{
			Type:          TypeRPCResponse,
			Header:        m.Header,
			Payload:       []byte(fmt.Sprintf("Unknown method '%s'.", m.Header)),
			CorrelationID: m.CorrelationID,
			Flags:         FlagError,
		})
		return
	}

	// The handler may block or call back, so it runs off the read loop - and therefore needs its
	// own copy of the payload.
	request := m
	request.Payload = append([]byte(nil), m.Payload...)

	go func() {
		result, err := handler(context.Background(), request)
		if err != nil {
			_ = c.Send(Message{
				Type:          TypeRPCResponse,
				Header:        request.Header,
				Payload:       []byte(err.Error()),
				CorrelationID: request.CorrelationID,
				Flags:         FlagError,
			})
			return
		}
		if request.Flags&FlagNoReply != 0 {
			return
		}
		_ = c.Send(Message{
			Type:          TypeRPCResponse,
			Header:        request.Header,
			Payload:       result,
			CorrelationID: request.CorrelationID,
		})
	}()
}

func (c *Client) deliverPublish(m Message) {
	c.mu.RLock()
	matched := make([]func(string, []byte), 0, 2)
	for _, s := range c.subscriptions {
		if TopicMatches(s.filter, m.Header) {
			matched = append(matched, s.handler)
		}
	}
	c.mu.RUnlock()

	for _, handler := range matched {
		handler(m.Header, m.Payload)
	}
}

func (c *Client) unpackBatch(m Message) {
	offset := 0
	for {
		inner, consumed, err := DecodeFrame(m.Payload[offset:], c.options.MaxFrameLength)
		if err != nil || consumed == 0 {
			return
		}
		offset += consumed
		// One level only: a nested envelope is a loop waiting to happen.
		if inner.Type != TypeBatch {
			c.dispatch(inner)
		}
	}
}

func (c *Client) handleStream(m Message) {
	switch m.Type {
	case TypeStreamStart:
		c.mu.Lock()
		c.streams[m.Header] = &reassembly{descriptor: DecodeStreamDescriptor(m.Payload)}
		c.mu.Unlock()

	case TypeStreamChunk:
		c.mu.Lock()
		state, found := c.streams[m.Header]
		if found {
			if m.CorrelationID != state.nextChunk {
				delete(c.streams, m.Header) // Out of order: abandon rather than corrupt.
			} else {
				state.nextChunk++
				state.buffer = append(state.buffer, m.Payload...)
			}
		}
		c.mu.Unlock()

	case TypeStreamEnd:
		c.mu.Lock()
		state, found := c.streams[m.Header]
		delete(c.streams, m.Header)
		handlers := append([]StreamHandler(nil), c.streamHandler...)
		c.mu.Unlock()

		if found {
			for _, handler := range handlers {
				handler(m.Header, state.descriptor, state.buffer)
			}
		}

	case TypeStreamAbort:
		c.mu.Lock()
		delete(c.streams, m.Header)
		c.mu.Unlock()
	}
}

// -------------------------------------------------------------- lifecycle

// Ping sends a keepalive probe and returns the round trip.
//
// The elapsed time is measured here rather than in the read loop so that time.Time keeps its
// monotonic reading.
//
// Beware the clock, not the network: time.Now on Windows resolves to roughly 500µs, which is
// coarser than a loopback round trip, so a single local probe can legitimately report 0. Use
// PingAverage when you need a figure you can act on.
func (c *Client) Ping(ctx context.Context) (time.Duration, error) {
	// Drop a stale answer from an earlier probe so this one measures its own reply.
	select {
	case <-c.pong:
	default:
	}

	started := time.Now()
	if err := c.Send(Message{Type: TypePing}); err != nil {
		return 0, err
	}

	select {
	case <-c.pong:
		elapsed := time.Since(started)
		c.Stats.lastRoundTrip.Store(elapsed.Nanoseconds())
		return elapsed, nil
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-c.closed:
		return 0, errors.New("blackhole: the connection closed before the probe was answered")
	}
}

// PingAverage sends count probes and returns the mean round trip.
//
// Timing the whole run rather than each probe keeps the figure meaningful on platforms whose clock
// is coarser than a single round trip - which on Windows loopback it usually is.
func (c *Client) PingAverage(ctx context.Context, count int) (time.Duration, error) {
	if count <= 0 {
		count = 10
	}

	started := time.Now()
	for i := 0; i < count; i++ {
		if _, err := c.Ping(ctx); err != nil {
			return 0, err
		}
	}
	elapsed := time.Since(started)

	average := elapsed / time.Duration(count)
	c.Stats.lastRoundTrip.Store(average.Nanoseconds())
	return average, nil
}

// Done returns a channel closed when the connection ends.
func (c *Client) Done() <-chan struct{} { return c.closed }

// Err returns the failure that ended the connection, or nil on a clean close.
func (c *Client) Err() error {
	if stored := c.closeErr.Load(); stored != nil {
		return *stored
	}
	return nil
}

// Close ends the connection and fails every pending call.
func (c *Client) Close() error {
	c.closeWith(nil)
	return c.conn.Close()
}

func (c *Client) closeWith(failure error) {
	c.closeOnce.Do(func() {
		if failure != nil {
			c.closeErr.Store(&failure)
		}
		close(c.closed)

		reason := "the connection closed before the reply arrived"
		if failure != nil {
			reason = failure.Error()
		}

		c.mu.Lock()
		pending := c.pending
		c.pending = make(map[int64]*pendingCall)
		c.mu.Unlock()

		for _, call := range pending {
			select {
			case call.result <- callResult{err: &RPCError{Method: call.method, Reason: reason}}:
			default:
			}
		}

		if failure != nil && c.options.OnError != nil {
			c.options.OnError(failure)
		}
	})
}

// PutInt32 writes a little-endian int32, matching how the .NET server encodes numeric replies.
func PutInt32(value int32) []byte {
	buffer := make([]byte, 4)
	binary.LittleEndian.PutUint32(buffer, uint32(value))
	return buffer
}

// Int32 reads a little-endian int32 from a payload.
func Int32(payload []byte) (int32, bool) {
	if len(payload) < 4 {
		return 0, false
	}
	return int32(binary.LittleEndian.Uint32(payload)), true
}
