// Package blackhole is a Go client for BlackHole Messaging: RPC, Pub/Sub, Streaming and Batching
// over a binary TCP protocol.
//
// BlackHole Messaging - Gravicode Studios, led by Kang Fadhil.
//
// This file is the Go counterpart of FrameCodec in the .NET library, and the only place in the
// package that knows the byte layout:
//
//	+----------------+------+-------+--------------+---------------+--------+---------+
//	| FrameLength(4) | Type | Flags | HeaderLen(2) | CorrelationId | Header | Payload |
//	|    int32 LE    |  u8  |  u8   |  uint16 LE   |    int64 LE   | UTF-8  |  bytes  |
//	+----------------+------+-------+--------------+---------------+--------+---------+
//	 \__ counts every byte after itself ________________________________________/
//
// Keeping encode and decode together is deliberate: the .NET v2 protocol kept two copies of its
// framing and they drifted apart. The interop suite runs this package against the real .NET server
// so a disagreement of one byte fails a test rather than a deployment.
package blackhole

import (
	"encoding/binary"
	"fmt"
	"strings"
)

// Frame layout constants.
const (
	// LengthPrefixSize is the size of the length prefix itself.
	LengthPrefixSize = 4
	// FixedHeaderSize is the number of bytes between the length prefix and the header text.
	FixedHeaderSize = 12
	// PrefixSize is the total number of bytes before the header text.
	PrefixSize = LengthPrefixSize + FixedHeaderSize
	// MaxHeaderLength is the largest UTF-8 header the two-byte length field can describe.
	MaxHeaderLength = 0xFFFF
	// DefaultMaxFrameLength caps a single frame while parsing.
	DefaultMaxFrameLength = 16 * 1024 * 1024
)

// MessageType identifies what a message means on the wire. The numeric values are part of the
// protocol contract and must never be reused.
type MessageType uint8

// Message types. Related types share a high nibble so a new streaming type gets 0x14 rather than
// whatever number happens to be free.
const (
	TypeNone MessageType = 0x00

	TypeRPCRequest  MessageType = 0x01
	TypeRPCResponse MessageType = 0x02

	TypePublish     MessageType = 0x03
	TypeSubscribe   MessageType = 0x04
	TypeAck         MessageType = 0x05
	TypeUnsubscribe MessageType = 0x06

	TypeStreamStart MessageType = 0x10
	TypeStreamChunk MessageType = 0x11
	TypeStreamEnd   MessageType = 0x12
	TypeStreamAbort MessageType = 0x13

	TypeBatch MessageType = 0x20

	TypePing MessageType = 0x30
	TypePong MessageType = 0x31
)

// String renders a message type for logs.
func (t MessageType) String() string {
	switch t {
	case TypeRPCRequest:
		return "RpcRequest"
	case TypeRPCResponse:
		return "RpcResponse"
	case TypePublish:
		return "Publish"
	case TypeSubscribe:
		return "Subscribe"
	case TypeAck:
		return "Ack"
	case TypeUnsubscribe:
		return "Unsubscribe"
	case TypeStreamStart:
		return "StreamStart"
	case TypeStreamChunk:
		return "StreamChunk"
	case TypeStreamEnd:
		return "StreamEnd"
	case TypeStreamAbort:
		return "StreamAbort"
	case TypeBatch:
		return "Batch"
	case TypePing:
		return "Ping"
	case TypePong:
		return "Pong"
	case TypeNone:
		return "None"
	default:
		return fmt.Sprintf("Unknown(0x%02X)", uint8(t))
	}
}

// MessageFlags carries per-message bit flags. One byte on the wire.
type MessageFlags uint8

// Message flags.
const (
	FlagNone       MessageFlags = 0
	FlagError      MessageFlags = 1 << 0
	FlagCompressed MessageFlags = 1 << 1
	FlagNoReply    MessageFlags = 1 << 2
)

// Message is one unit crossing the wire.
//
// Header and CorrelationID are overloaded by Type: the header is an RPC method name, a topic, or a
// stream id, and the correlation id matches a reply to its request, indexes a stream chunk, or
// counts the messages inside a batch.
type Message struct {
	Type          MessageType
	Flags         MessageFlags
	CorrelationID int64
	Header        string
	Payload       []byte
}

// IsError reports whether the peer sent a failure instead of a result.
func (m Message) IsError() bool { return m.Flags&FlagError != 0 }

// Text decodes the payload as UTF-8.
func (m Message) Text() string { return string(m.Payload) }

// String renders a message for logs.
func (m Message) String() string {
	return fmt.Sprintf("<%s %q %dB #%d>", m.Type, m.Header, len(m.Payload), m.CorrelationID)
}

// ProtocolError means bytes on the wire cannot form a valid frame. It is always fatal for the
// connection that produced it: once framing is lost there is no way to resynchronise.
type ProtocolError struct {
	Reason string
}

func (e *ProtocolError) Error() string { return "blackhole: " + e.Reason }

// RPCError is returned when a remote method is unknown, fails, times out, or its connection drops.
type RPCError struct {
	Method  string
	Reason  string
}

func (e *RPCError) Error() string {
	return fmt.Sprintf("blackhole: rpc %q: %s", e.Method, e.Reason)
}

// StreamDescriptor is the metadata carried by a StreamStart message, so a receiver knows what is
// coming before the first chunk lands.
type StreamDescriptor struct {
	Name        string
	TotalLength int64 // UnknownLength when the sender does not know the size up front.
	ContentType string
}

// UnknownLength marks a stream whose size is not known in advance.
const UnknownLength int64 = -1

// HasLength reports whether the sender declared a size.
func (d StreamDescriptor) HasLength() bool { return d.TotalLength >= 0 }

// Encode serialises the descriptor to the StreamStart payload layout.
func (d StreamDescriptor) Encode() []byte {
	name := []byte(d.Name)
	contentType := []byte(d.ContentType)

	buffer := make([]byte, 0, 12+len(name)+len(contentType))
	buffer = binary.LittleEndian.AppendUint64(buffer, uint64(d.TotalLength))
	buffer = binary.LittleEndian.AppendUint16(buffer, uint16(len(name)))
	buffer = append(buffer, name...)
	buffer = binary.LittleEndian.AppendUint16(buffer, uint16(len(contentType)))
	buffer = append(buffer, contentType...)
	return buffer
}

// DecodeStreamDescriptor parses a StreamStart payload. A short payload yields an unnamed descriptor
// with an unknown length.
func DecodeStreamDescriptor(payload []byte) StreamDescriptor {
	if len(payload) < 10 {
		return StreamDescriptor{TotalLength: UnknownLength}
	}

	total := int64(binary.LittleEndian.Uint64(payload))
	nameLength := int(binary.LittleEndian.Uint16(payload[8:]))
	if len(payload) < 10+nameLength+2 {
		return StreamDescriptor{TotalLength: total}
	}

	name := string(payload[10 : 10+nameLength])
	contentTypeLength := int(binary.LittleEndian.Uint16(payload[10+nameLength:]))
	start := 12 + nameLength

	contentType := ""
	if len(payload) >= start+contentTypeLength {
		contentType = string(payload[start : start+contentTypeLength])
	}

	return StreamDescriptor{Name: name, TotalLength: total, ContentType: contentType}
}

// EncodeFrame serialises one message, length prefix included.
func EncodeFrame(m Message) ([]byte, error) {
	header := []byte(m.Header)
	if len(header) > MaxHeaderLength {
		return nil, &ProtocolError{Reason: fmt.Sprintf(
			"header is %d bytes; the limit is %d", len(header), MaxHeaderLength)}
	}

	frameLength := FixedHeaderSize + len(header) + len(m.Payload)
	if frameLength > 0x7FFFFFFF {
		return nil, &ProtocolError{Reason: "frame exceeds int32 addressing"}
	}

	frame := make([]byte, PrefixSize+len(header)+len(m.Payload))
	binary.LittleEndian.PutUint32(frame, uint32(frameLength))
	frame[4] = uint8(m.Type)
	frame[5] = uint8(m.Flags)
	binary.LittleEndian.PutUint16(frame[6:], uint16(len(header)))
	binary.LittleEndian.PutUint64(frame[8:], uint64(m.CorrelationID))
	copy(frame[PrefixSize:], header)
	copy(frame[PrefixSize+len(header):], m.Payload)
	return frame, nil
}

// DecodeFrame parses one frame from the front of buffer.
//
// It returns the message and how many bytes it consumed. When the buffer does not yet hold a whole
// frame it returns consumed == 0 and a nil error, so a caller should read more and try again.
//
// The returned Payload aliases buffer. Copy it if it must outlive the read buffer.
func DecodeFrame(buffer []byte, maxFrameLength int) (Message, int, error) {
	if len(buffer) < LengthPrefixSize {
		return Message{}, 0, nil
	}

	frameLength := int(int32(binary.LittleEndian.Uint32(buffer)))
	if frameLength < FixedHeaderSize {
		return Message{}, 0, &ProtocolError{Reason: fmt.Sprintf(
			"frame length %d is below the %d-byte minimum; the stream is out of sync",
			frameLength, FixedHeaderSize)}
	}
	if frameLength > maxFrameLength {
		return Message{}, 0, &ProtocolError{Reason: fmt.Sprintf(
			"frame length %d exceeds the %d-byte limit", frameLength, maxFrameLength)}
	}
	if len(buffer) < LengthPrefixSize+frameLength {
		return Message{}, 0, nil
	}

	headerLength := int(binary.LittleEndian.Uint16(buffer[6:]))
	if FixedHeaderSize+headerLength > frameLength {
		return Message{}, 0, &ProtocolError{Reason: fmt.Sprintf(
			"header length %d does not fit in a %d-byte frame", headerLength, frameLength)}
	}

	payloadStart := PrefixSize + headerLength
	payloadLength := frameLength - FixedHeaderSize - headerLength

	message := Message{
		Type:          MessageType(buffer[4]),
		Flags:         MessageFlags(buffer[5]),
		CorrelationID: int64(binary.LittleEndian.Uint64(buffer[8:])),
		Header:        string(buffer[PrefixSize:payloadStart]),
	}
	if payloadLength > 0 {
		message.Payload = buffer[payloadStart : payloadStart+payloadLength]
	}

	return message, LengthPrefixSize + frameLength, nil
}

// TopicMatches reports whether topic matches an MQTT-style filter, where "+" matches exactly one
// segment and "#" matches the remainder.
//
// This mirrors TopicFilter.Matches in the .NET library, including one place it is stricter than the
// MQTT specification: "sensor/#" does not match the bare parent topic "sensor", because "#" must
// have at least one segment to swallow. The broker decides delivery, so agreeing with it matters
// more than agreeing with the spec.
func TopicMatches(filter, topic string) bool {
	filterParts := strings.Split(filter, "/")
	topicParts := strings.Split(topic, "/")

	for index, part := range filterParts {
		if part == "#" {
			return index < len(topicParts)
		}
		if index >= len(topicParts) {
			return false
		}
		if part != "+" && part != topicParts[index] {
			return false
		}
	}

	return len(filterParts) == len(topicParts)
}
