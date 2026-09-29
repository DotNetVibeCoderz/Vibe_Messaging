package io.gravicode.bigpipe;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Batching producer. Records sent to a topic within {@code linger} travel in one request.
 *
 * <pre>{@code
 * try (var producer = Producer.builder().bootstrap("localhost:9092").build()) {
 *     RecordMetadata m = producer.send("orders", "order-1", "{\"id\":1}").join();
 * }
 * }</pre>
 */
public final class Producer implements AutoCloseable {
    private final BigPipe.Http http;
    private final String compression;
    private final int maxBatch;
    private final Map<String, List<Pending>> pending = new HashMap<>();
    private final ScheduledExecutorService scheduler;
    private volatile boolean closed;

    private record Pending(ObjectNode record, CompletableFuture<RecordMetadata> future) {
    }

    private Producer(Builder b) {
        this.http = new BigPipe.Http(BigPipe.gatewayUrl(b.url, b.bootstrap), null, Duration.ofSeconds(60));
        this.compression = b.compression;
        this.maxBatch = Math.max(1, b.maxBatch);
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "bigpipe-producer");
            t.setDaemon(true);
            return t;
        });
        long linger = Math.max(1, b.linger.toMillis());
        scheduler.scheduleWithFixedDelay(this::flushQuietly, linger, linger, TimeUnit.MILLISECONDS);
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Sends a UTF-8 key/value. */
    public CompletableFuture<RecordMetadata> send(String topic, String key, String value) {
        return send(topic, key == null ? null : key.getBytes(StandardCharsets.UTF_8),
                value == null ? null : value.getBytes(StandardCharsets.UTF_8), null, null);
    }

    public CompletableFuture<RecordMetadata> send(String topic, String key, String value, Map<String, String> headers) {
        return send(topic, key == null ? null : key.getBytes(StandardCharsets.UTF_8),
                value == null ? null : value.getBytes(StandardCharsets.UTF_8), headers, null);
    }

    /** Sends raw bytes; {@code partition} null = key hash (Kafka-compatible) or round-robin. */
    public CompletableFuture<RecordMetadata> send(String topic, byte[] key, byte[] value, Map<String, String> headers, Integer partition) {
        if (closed) throw new IllegalStateException("producer is closed");
        CompletableFuture<RecordMetadata> f = new CompletableFuture<>();
        boolean full;
        synchronized (pending) {
            List<Pending> list = pending.computeIfAbsent(topic, t -> new ArrayList<>());
            list.add(new Pending(BigPipe.encodeRecord(key, value, headers, partition, null), f));
            full = list.size() >= maxBatch;
        }
        if (full) scheduler.execute(this::flushQuietly);
        return f;
    }

    private void flushQuietly() {
        try {
            flush();
        } catch (RuntimeException ignored) {
            // failures are delivered through the futures
        }
    }

    /** Sends everything queued now and waits for the responses. */
    public void flush() {
        Map<String, List<Pending>> batches;
        synchronized (pending) {
            if (pending.isEmpty()) return;
            batches = new HashMap<>(pending);
            pending.clear();
        }
        batches.forEach((topic, items) -> {
            for (int i = 0; i < items.size(); i += maxBatch) post(topic, items.subList(i, Math.min(items.size(), i + maxBatch)));
        });
    }

    private void post(String topic, List<Pending> batch) {
        try {
            ObjectNode body = BigPipe.JSON.createObjectNode();
            ArrayNode recs = body.putArray("records");
            batch.forEach(p -> recs.add(p.record()));
            if (compression != null) body.put("compression", compression);
            JsonNode res = http.call("POST", "/v1/topics/" + BigPipe.enc(topic) + "/records", body);
            JsonNode offsets = res.path("offsets");
            for (int i = 0; i < batch.size(); i++) {
                JsonNode o = offsets.get(i);
                batch.get(i).future().complete(new RecordMetadata(topic, o.path("partition").asInt(), o.path("offset").asLong()));
            }
        } catch (RuntimeException e) {
            batch.forEach(p -> p.future().completeExceptionally(e));
        }
    }

    @Override
    public void close() {
        closed = true;
        scheduler.shutdown();
        try {
            scheduler.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        flush();
    }

    public static final class Builder {
        private String url;
        private String bootstrap;
        private String compression;
        private Duration linger = Duration.ofMillis(5);
        private int maxBatch = 500;

        /** Kafka bootstrap list; the gateway is assumed on the first host at port 8082. */
        public Builder bootstrap(String bootstrap) {
            this.bootstrap = bootstrap;
            return this;
        }

        public Builder url(String gatewayUrl) {
            this.url = gatewayUrl;
            return this;
        }

        /** none, gzip, snappy, lz4 or zstd (applied to the stored batch). */
        public Builder compression(String codec) {
            this.compression = "none".equals(codec) ? null : codec;
            return this;
        }

        public Builder linger(Duration linger) {
            this.linger = linger;
            return this;
        }

        public Builder maxBatch(int maxBatch) {
            this.maxBatch = maxBatch;
            return this;
        }

        public Producer build() {
            return new Producer(this);
        }
    }
}
