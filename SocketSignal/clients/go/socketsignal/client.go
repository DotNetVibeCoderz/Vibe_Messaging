// Package socketsignal is a Go client for SocketSignal, a bidirectional RPC protocol over
// WebSockets.
//
// Built by Gravicode Studios, led by Kang Fadhil.
//
// A client calls server methods and gets return values back, and the server can call methods
// registered here:
//
//	client := socketsignal.New(socketsignal.Options{})
//	client.On("serverHello", func(args []json.RawMessage) (any, error) {
//		var text string
//		_ = json.Unmarshal(args[0], &text)
//		return "go heard " + text, nil
//	})
//
//	if err := client.Connect(ctx, "ws://localhost:8080/ws/"); err != nil {
//		log.Fatal(err)
//	}
//	defer client.Close()
//
//	var total int
//	if err := client.Call(ctx, &total, "sum", 5, 7); err != nil {
//		log.Fatal(err)
//	}
package socketsignal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
)

// Handler runs a method the server called. Its return value becomes the reply when the server
// asked for one; a non-nil error is sent back as an error instead.
type Handler func(args []json.RawMessage) (any, error)

// InvocationError means the remote handler ran and failed. Stack traces never cross the wire.
type InvocationError struct {
	Method  string
	Message string
}

func (e *InvocationError) Error() string {
	return fmt.Sprintf("remote method %q failed: %s", e.Method, e.Message)
}

// TimeoutError means the reply did not arrive inside CallTimeout.
type TimeoutError struct {
	Method  string
	Timeout time.Duration
}

func (e *TimeoutError) Error() string {
	return fmt.Sprintf("remote method %q did not answer within %s", e.Method, e.Timeout)
}

// ErrClosed is returned when the socket goes away with calls still in flight, and by calls made
// on a client that is not connected. Pending calls fail with it rather than hanging.
var ErrClosed = errors.New("socketsignal: the connection is closed")

// Options tunes a Client. The zero value is usable: it fills in the defaults below.
type Options struct {
	// CallTimeout is how long Call waits for a reply. Zero means 30s; negative waits forever.
	CallTimeout time.Duration

	// KeepAlive is the protocol ping interval. Zero means 15s; negative disables pings.
	KeepAlive time.Duration

	// MaxMessageSize caps a single frame. Zero means 4 MiB.
	MaxMessageSize int64

	// OnConnected, if set, is called with the assigned client id after the welcome frame.
	OnConnected func(clientID string)

	// OnDisconnected, if set, is called with the reason the socket closed.
	OnDisconnected func(reason string)
}

func (o *Options) applyDefaults() {
	if o.CallTimeout == 0 {
		o.CallTimeout = 30 * time.Second
	}
	if o.KeepAlive == 0 {
		o.KeepAlive = 15 * time.Second
	}
	if o.MaxMessageSize == 0 {
		o.MaxMessageSize = 4 << 20
	}
}

// frame is the whole protocol: one JSON object per WebSocket message.
type frame struct {
	Type         string            `json:"type"`
	ID           string            `json:"id,omitempty"`
	Method       string            `json:"method,omitempty"`
	Args         []json.RawMessage `json:"args,omitempty"`
	ExpectReturn bool              `json:"expectReturn,omitempty"`
	Result       json.RawMessage   `json:"result,omitempty"`
	Error        string            `json:"error,omitempty"`
}

type pendingCall struct {
	method string
	done   chan frame
}

// Client is a SocketSignal connection. It is safe for concurrent use.
type Client struct {
	options Options

	mu       sync.Mutex
	conn     *websocket.Conn
	handlers map[string]Handler
	pending  map[string]*pendingCall
	closed   bool

	nextID   atomic.Int64
	clientID atomic.Value // string

	welcomed chan string
	stop     chan struct{}
	stopOnce sync.Once
}

// New builds a client. It does not dial anything - call Connect.
func New(options Options) *Client {
	options.applyDefaults()
	return &Client{
		options:  options,
		handlers: make(map[string]Handler),
		pending:  make(map[string]*pendingCall),
	}
}

// On registers a method the server may call. Registering the same name twice replaces it.
func (c *Client) On(method string, handler Handler) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.handlers[method] = handler
}

// Off removes a registration.
func (c *Client) Off(method string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.handlers, method)
}

// ClientID is the id the server assigned in its welcome frame, or "" before connecting.
func (c *Client) ClientID() string {
	if id, ok := c.clientID.Load().(string); ok {
		return id
	}
	return ""
}

// Connect dials the server and returns once the welcome frame has arrived.
func (c *Client) Connect(ctx context.Context, url string) error {
	conn, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		return fmt.Errorf("socketsignal: dial %s: %w", url, err)
	}
	conn.SetReadLimit(c.options.MaxMessageSize)

	c.mu.Lock()
	c.conn = conn
	c.closed = false
	c.mu.Unlock()

	c.welcomed = make(chan string, 1)
	c.stop = make(chan struct{})
	c.stopOnce = sync.Once{}

	go c.readLoop()
	if c.options.KeepAlive > 0 {
		go c.keepAliveLoop()
	}

	timeout := c.options.CallTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}

	select {
	case id := <-c.welcomed:
		c.clientID.Store(id)
		if c.options.OnConnected != nil {
			c.options.OnConnected(id)
		}
		return nil
	case <-time.After(timeout):
		_ = c.Close()
		return errors.New("socketsignal: the server did not send a welcome frame")
	case <-ctx.Done():
		_ = c.Close()
		return ctx.Err()
	}
}

// Call invokes a server method and decodes its return value into result, which may be nil when
// the reply is not wanted.
func (c *Client) Call(ctx context.Context, result any, method string, args ...any) error {
	conn := c.connection()
	if conn == nil {
		return ErrClosed
	}

	id := c.mintID()
	call := &pendingCall{method: method, done: make(chan frame, 1)}

	c.mu.Lock()
	c.pending[id] = call
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
	}()

	if err := c.write(ctx, conn, frame{
		Type:         "invoke",
		ID:           id,
		Method:       method,
		Args:         encodeArgs(args),
		ExpectReturn: true,
	}); err != nil {
		return err
	}

	var timeout <-chan time.Time
	if c.options.CallTimeout > 0 {
		timer := time.NewTimer(c.options.CallTimeout)
		defer timer.Stop()
		timeout = timer.C
	}

	select {
	case reply := <-call.done:
		if reply.Type == "" {
			return ErrClosed
		}
		if reply.Error != "" {
			return &InvocationError{Method: method, Message: reply.Error}
		}
		if result == nil || len(reply.Result) == 0 {
			return nil
		}
		return json.Unmarshal(reply.Result, result)
	case <-timeout:
		return &TimeoutError{Method: method, Timeout: c.options.CallTimeout}
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Send invokes a server method without waiting for a reply.
func (c *Client) Send(ctx context.Context, method string, args ...any) error {
	conn := c.connection()
	if conn == nil {
		return ErrClosed
	}
	return c.write(ctx, conn, frame{
		Type:   "invoke",
		ID:     c.mintID(),
		Method: method,
		Args:   encodeArgs(args),
	})
}

// Close shuts the connection down and fails everything still waiting on it.
func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	conn := c.conn
	c.mu.Unlock()

	c.halt()
	if conn == nil {
		return nil
	}
	return conn.Close(websocket.StatusNormalClosure, "")
}

// ---------------------------------------------------------------------------------------------

func (c *Client) readLoop() {
	reason := "closed by peer"
	ctx := context.Background()

	for {
		conn := c.connection()
		if conn == nil {
			break
		}

		_, data, err := conn.Read(ctx)
		if err != nil {
			// A blocking Read that fails because Close() ran is a clean shutdown, not a
			// transport fault: report it as one rather than surfacing the socket's error.
			if c.isClosed() {
				reason = "closed by client"
			} else {
				reason = err.Error()
			}
			break
		}

		var f frame
		if err := json.Unmarshal(data, &f); err != nil {
			continue // a frame this client cannot read is not a reason to drop the link
		}
		c.dispatch(ctx, f)
	}

	c.failPending()
	c.halt()
	if c.options.OnDisconnected != nil {
		c.options.OnDisconnected(reason)
	}
}

func (c *Client) dispatch(ctx context.Context, f frame) {
	switch f.Type {
	case "welcome":
		select {
		case c.welcomed <- f.ID:
		default:
		}

	case "invoke":
		go c.invoke(ctx, f)

	case "result":
		c.mu.Lock()
		call := c.pending[f.ID]
		delete(c.pending, f.ID)
		c.mu.Unlock()
		if call != nil {
			call.done <- f
		}

	case "ping":
		conn := c.connection()
		if conn != nil {
			_ = c.write(ctx, conn, frame{Type: "pong", ID: f.ID})
		}
	}
}

func (c *Client) invoke(ctx context.Context, f frame) {
	c.mu.Lock()
	handler := c.handlers[f.Method]
	c.mu.Unlock()

	conn := c.connection()
	if conn == nil {
		return
	}

	if handler == nil {
		if f.ExpectReturn {
			_ = c.write(ctx, conn, frame{
				Type:  "result",
				ID:    f.ID,
				Error: fmt.Sprintf("Method '%s' not found", f.Method),
			})
		}
		return
	}

	value, err := handler(f.Args)
	if !f.ExpectReturn {
		return
	}
	if err != nil {
		_ = c.write(ctx, conn, frame{Type: "result", ID: f.ID, Error: err.Error()})
		return
	}

	encoded, marshalErr := json.Marshal(value)
	if marshalErr != nil {
		_ = c.write(ctx, conn, frame{Type: "result", ID: f.ID, Error: marshalErr.Error()})
		return
	}
	_ = c.write(ctx, conn, frame{Type: "result", ID: f.ID, Result: encoded})
}

func (c *Client) keepAliveLoop() {
	ticker := time.NewTicker(c.options.KeepAlive)
	defer ticker.Stop()

	for {
		select {
		case <-c.stop:
			return
		case <-ticker.C:
			conn := c.connection()
			if conn == nil {
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err := c.write(ctx, conn, frame{Type: "ping", ID: c.mintID()})
			cancel()
			if err != nil {
				return
			}
		}
	}
}

func (c *Client) write(ctx context.Context, conn *websocket.Conn, f frame) error {
	data, err := json.Marshal(f)
	if err != nil {
		return err
	}
	return conn.Write(ctx, websocket.MessageText, data)
}

func (c *Client) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

func (c *Client) connection() *websocket.Conn {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	return c.conn
}

func (c *Client) failPending() {
	c.mu.Lock()
	pending := c.pending
	c.pending = make(map[string]*pendingCall)
	c.mu.Unlock()

	for _, call := range pending {
		call.done <- frame{} // an empty frame means "the socket went away"
	}
}

func (c *Client) halt() {
	c.stopOnce.Do(func() {
		if c.stop != nil {
			close(c.stop)
		}
	})
}

func (c *Client) mintID() string {
	return strconv.FormatInt(c.nextID.Add(1), 10)
}

func encodeArgs(args []any) []json.RawMessage {
	if len(args) == 0 {
		return []json.RawMessage{}
	}
	encoded := make([]json.RawMessage, 0, len(args))
	for _, arg := range args {
		data, err := json.Marshal(arg)
		if err != nil {
			data = []byte("null")
		}
		encoded = append(encoded, data)
	}
	return encoded
}
