// BlackHole Messaging - Gravicode Studios, led by Kang Fadhil.
//
// Codec tests that need no server.

package blackhole

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"
)

func TestRoundTripsEveryField(t *testing.T) {
	original := Message{
		Type:          TypeRPCRequest,
		Flags:         FlagNoReply,
		CorrelationID: 987654321,
		Header:        "sensor/tank-3/temperature",
		Payload:       []byte("28.4"),
	}

	frame, err := EncodeFrame(original)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	parsed, consumed, err := DecodeFrame(frame, DefaultMaxFrameLength)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if consumed != len(frame) {
		t.Errorf("consumed %d bytes, frame is %d", consumed, len(frame))
	}
	if parsed.Type != original.Type || parsed.Flags != original.Flags {
		t.Errorf("type/flags: got %v/%v, want %v/%v", parsed.Type, parsed.Flags, original.Type, original.Flags)
	}
	if parsed.CorrelationID != original.CorrelationID {
		t.Errorf("correlation: got %d, want %d", parsed.CorrelationID, original.CorrelationID)
	}
	if parsed.Header != original.Header {
		t.Errorf("header: got %q, want %q", parsed.Header, original.Header)
	}
	if !bytes.Equal(parsed.Payload, original.Payload) {
		t.Errorf("payload: got %q, want %q", parsed.Payload, original.Payload)
	}
}

func TestFrameLayoutMatchesTheSpecification(t *testing.T) {
	frame, err := EncodeFrame(Message{Type: TypePublish, Header: "ab", Payload: []byte("xyz"), CorrelationID: 7})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	if got := binary.LittleEndian.Uint32(frame); got != FixedHeaderSize+2+3 {
		t.Errorf("length prefix: got %d, want %d", got, FixedHeaderSize+2+3)
	}
	if frame[4] != uint8(TypePublish) {
		t.Errorf("type byte: got %d", frame[4])
	}
	if frame[5] != 0 {
		t.Errorf("flags byte: got %d", frame[5])
	}
	if got := binary.LittleEndian.Uint16(frame[6:]); got != 2 {
		t.Errorf("header length: got %d, want 2", got)
	}
	if got := int64(binary.LittleEndian.Uint64(frame[8:])); got != 7 {
		t.Errorf("correlation id: got %d, want 7", got)
	}
	if string(frame[PrefixSize:PrefixSize+2]) != "ab" {
		t.Errorf("header bytes: got %q", frame[PrefixSize:PrefixSize+2])
	}
	if string(frame[PrefixSize+2:]) != "xyz" {
		t.Errorf("payload bytes: got %q", frame[PrefixSize+2:])
	}
}

func TestHandlesEmptyHeaderAndPayload(t *testing.T) {
	frame, _ := EncodeFrame(Message{Type: TypePing})
	parsed, _, err := DecodeFrame(frame, DefaultMaxFrameLength)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if parsed.Type != TypePing || parsed.Header != "" || len(parsed.Payload) != 0 {
		t.Errorf("got %v", parsed)
	}
}

func TestReturnsNothingUntilTheWholeFrameArrives(t *testing.T) {
	frame, _ := EncodeFrame(Message{Type: TypePublish, Header: "topic", Payload: []byte("body")})

	for prefix := 0; prefix < len(frame); prefix++ {
		_, consumed, err := DecodeFrame(frame[:prefix], DefaultMaxFrameLength)
		if err != nil {
			t.Fatalf("prefix %d: unexpected error %v", prefix, err)
		}
		if consumed != 0 {
			t.Fatalf("prefix %d: consumed %d bytes from a partial frame", prefix, consumed)
		}
	}

	if _, consumed, _ := DecodeFrame(frame, DefaultMaxFrameLength); consumed != len(frame) {
		t.Errorf("complete frame: consumed %d, want %d", consumed, len(frame))
	}
}

func TestParsesBackToBackFrames(t *testing.T) {
	var stream []byte
	for i := 0; i < 5; i++ {
		frame, _ := EncodeFrame(Message{Type: TypePublish, Header: "topic/" + string(rune('0'+i))})
		stream = append(stream, frame...)
	}

	var seen []string
	offset := 0
	for {
		message, consumed, err := DecodeFrame(stream[offset:], DefaultMaxFrameLength)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if consumed == 0 {
			break
		}
		offset += consumed
		seen = append(seen, message.Header)
	}

	if len(seen) != 5 || offset != len(stream) {
		t.Errorf("parsed %d frames over %d/%d bytes", len(seen), offset, len(stream))
	}
}

func TestHandlesNonASCII(t *testing.T) {
	original := Message{
		Type:    TypePublish,
		Header:  "suhu/tangki/derajat-°C",
		Payload: []byte("28,4 °C — 日本語 — 🕳"),
	}

	frame, _ := EncodeFrame(original)
	parsed, _, err := DecodeFrame(frame, DefaultMaxFrameLength)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if parsed.Header != original.Header || !bytes.Equal(parsed.Payload, original.Payload) {
		t.Errorf("got header %q payload %q", parsed.Header, parsed.Payload)
	}
}

func TestRejectsAFrameLongerThanTheLimit(t *testing.T) {
	frame, _ := EncodeFrame(Message{Type: TypePublish, Header: "t", Payload: make([]byte, 4096)})
	if _, _, err := DecodeFrame(frame, 128); err == nil {
		t.Fatal("expected an error for an oversized frame")
	} else if !strings.Contains(err.Error(), "exceeds") {
		t.Errorf("unexpected message: %v", err)
	}
}

func TestRejectsAnImpossibleLengthPrefix(t *testing.T) {
	if _, _, err := DecodeFrame([]byte{2, 0, 0, 0, 1, 2}, DefaultMaxFrameLength); err == nil {
		t.Fatal("expected an error for an impossible length prefix")
	} else if !strings.Contains(err.Error(), "out of sync") {
		t.Errorf("unexpected message: %v", err)
	}
}

func TestRejectsAnOversizedHeader(t *testing.T) {
	if _, err := EncodeFrame(Message{Type: TypePublish, Header: strings.Repeat("x", 70000)}); err == nil {
		t.Fatal("expected an error for an oversized header")
	}
}

func TestPreservesAnUnknownMessageType(t *testing.T) {
	// A newer peer may know message types this client does not; that is not a framing error.
	frame, _ := EncodeFrame(Message{Type: TypePublish, Header: "t"})
	frame[4] = 0x7E

	parsed, _, err := DecodeFrame(frame, DefaultMaxFrameLength)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if parsed.Type != MessageType(0x7E) {
		t.Errorf("got type %v", parsed.Type)
	}
	if got := parsed.Type.String(); got != "Unknown(0x7E)" {
		t.Errorf("String(): got %q", got)
	}
}

func TestStreamDescriptorRoundTrips(t *testing.T) {
	original := StreamDescriptor{Name: "kalibrasi-2026.csv", TotalLength: 1048576, ContentType: "text/csv"}
	parsed := DecodeStreamDescriptor(original.Encode())

	if parsed != original {
		t.Errorf("got %+v, want %+v", parsed, original)
	}
	if !parsed.HasLength() {
		t.Error("expected a known length")
	}
}

func TestStreamDescriptorHandlesUnknownLength(t *testing.T) {
	original := StreamDescriptor{Name: "live.log", TotalLength: UnknownLength, ContentType: "text/plain"}
	if parsed := DecodeStreamDescriptor(original.Encode()); parsed != original {
		t.Errorf("got %+v, want %+v", parsed, original)
	}
	if DecodeStreamDescriptor(nil).HasLength() {
		t.Error("an empty payload should decode as an unknown length")
	}
}

func TestTopicMatching(t *testing.T) {
	cases := []struct {
		filter, topic string
		want          bool
	}{
		{"sensor/tank-3/temp", "sensor/tank-3/temp", true},
		{"sensor/+/temp", "sensor/tank-3/temp", true},
		{"sensor/+/temp", "sensor/tank-3/humidity", false},
		{"sensor/+/temp", "sensor/a/b/temp", false},
		{"sensor/#", "sensor/tank-3/temp", true},
		{"sensor/#", "sensor", false},
		{"#", "anything/at/all", true},
		{"sensor/tank-3/temp", "sensor/tank-3", false},
		{"sensor/tank-3", "sensor/tank-3/temp", false},
		{"+/+/temp", "sensor/tank-3/temp", true},
	}

	for _, c := range cases {
		if got := TopicMatches(c.filter, c.topic); got != c.want {
			t.Errorf("TopicMatches(%q, %q) = %v, want %v", c.filter, c.topic, got, c.want)
		}
	}
}

func BenchmarkEncodeFrame(b *testing.B) {
	message := Message{Type: TypePublish, Header: "sensor/tank-3/temperature", Payload: []byte("28.4")}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := EncodeFrame(message); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeFrame(b *testing.B) {
	frame, _ := EncodeFrame(Message{Type: TypePublish, Header: "sensor/tank-3/temperature", Payload: []byte("28.4")})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := DecodeFrame(frame, DefaultMaxFrameLength); err != nil {
			b.Fatal(err)
		}
	}
}
