package io.gravicode.bigpipe;

import com.fasterxml.jackson.databind.JsonNode;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A consumed record. Key and value are bytes; use {@link #valueString()} or {@link #json()} for text.
 */
public class BigPipeRecord {
    private final String topic;
    private final int partition;
    private final long offset;
    private final long timestamp;
    private final byte[] key;
    private final byte[] value;
    private final Map<String, String> headers;
    private final int deliveryCount;

    BigPipeRecord(String topic, int partition, long offset, long timestamp, byte[] key, byte[] value, Map<String, String> headers, int deliveryCount) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.timestamp = timestamp;
        this.key = key;
        this.value = value;
        this.headers = headers;
        this.deliveryCount = deliveryCount;
    }

    static byte[] decode(JsonNode v, String encoding) {
        if (v == null || v.isNull() || v.isMissingNode()) return null;
        return "base64".equals(encoding) ? Base64.getDecoder().decode(v.asText()) : v.asText().getBytes(StandardCharsets.UTF_8);
    }

    static BigPipeRecord fromJson(JsonNode d) {
        Map<String, String> h = new LinkedHashMap<>();
        d.path("headers").fields().forEachRemaining(e -> h.put(e.getKey(), e.getValue().isNull() ? null : e.getValue().asText()));
        return new BigPipeRecord(d.path("topic").asText(), d.path("partition").asInt(), d.path("offset").asLong(), d.path("timestamp").asLong(),
                decode(d.get("key"), d.path("key_encoding").asText("utf8")), decode(d.get("value"), d.path("value_encoding").asText("utf8")),
                Collections.unmodifiableMap(h), d.path("delivery_count").asInt(1));
    }

    public String topic() { return topic; }
    public int partition() { return partition; }
    public long offset() { return offset; }
    public Instant timestamp() { return Instant.ofEpochMilli(timestamp); }
    public byte[] key() { return key; }
    public byte[] value() { return value; }
    public Map<String, String> headers() { return headers; }
    public int deliveryCount() { return deliveryCount; }

    public String keyString() {
        return key == null ? null : new String(key, StandardCharsets.UTF_8);
    }

    public String valueString() {
        return value == null ? null : new String(value, StandardCharsets.UTF_8);
    }

    /** Parses the value as JSON. */
    public JsonNode json() {
        try {
            return value == null ? BigPipe.JSON.nullNode() : BigPipe.JSON.readTree(value);
        } catch (Exception e) {
            throw new IllegalStateException("value is not JSON", e);
        }
    }

    @Override
    public String toString() {
        return topic + "-" + partition + "@" + offset + " " + keyString() + " " + valueString();
    }
}
