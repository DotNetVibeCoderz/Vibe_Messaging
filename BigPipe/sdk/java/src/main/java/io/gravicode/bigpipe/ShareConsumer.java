package io.gravicode.bigpipe;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Queue-style consumer on a share group: records are locked to one worker, acknowledged
 * individually and moved to a dead-letter topic after too many attempts.
 */
public final class ShareConsumer implements AutoCloseable {
    private final BigPipe.Http http;
    private final Builder cfg;
    private final List<ObjectNode> acks = new ArrayList<>();

    /** A record locked to this worker. */
    public final class ShareRecord {
        private final BigPipeRecord record;

        ShareRecord(BigPipeRecord record) {
            this.record = record;
        }

        public BigPipeRecord record() { return record; }
        public String valueString() { return record.valueString(); }
        public int deliveryCount() { return record.deliveryCount(); }

        public void accept() { ack(record, "accept"); }
        /** Redeliver to any worker. */
        public void release() { ack(record, "release"); }
        /** Give up; goes to the dead-letter topic when configured. */
        public void reject() { ack(record, "reject"); }
    }

    private ShareConsumer(Builder b) {
        this.cfg = b;
        this.http = new BigPipe.Http(BigPipe.gatewayUrl(b.url, b.bootstrap), null, Duration.ofSeconds(90));
    }

    public static Builder builder() {
        return new Builder();
    }

    private synchronized void ack(BigPipeRecord r, String action) {
        ObjectNode a = BigPipe.JSON.createObjectNode();
        a.put("topic", r.topic());
        a.put("partition", r.partition());
        a.put("offset", r.offset());
        a.put("action", action);
        acks.add(a);
    }

    /** Sends pending acknowledgements (also done before every poll). */
    public synchronized void flushAcks() {
        if (acks.isEmpty()) return;
        ObjectNode body = BigPipe.JSON.createObjectNode();
        body.put("member", cfg.member);
        ArrayNode arr = body.putArray("acks");
        acks.forEach(arr::add);
        acks.clear();
        http.call("POST", "/v1/share/" + BigPipe.enc(cfg.group) + "/ack", body);
    }

    public List<ShareRecord> poll(Duration timeout) {
        flushAcks();
        ObjectNode body = BigPipe.JSON.createObjectNode();
        body.put("member", cfg.member);
        body.putPOJO("topics", cfg.topics);
        body.put("max_records", cfg.maxRecords);
        body.put("lock_ms", cfg.lock.toMillis());
        body.put("max_attempts", cfg.maxAttempts);
        if (cfg.deadLetterTopic != null) body.put("dlq_topic", cfg.deadLetterTopic);
        if (cfg.filter != null) body.put("filter", cfg.filter);
        body.put("timeout_ms", timeout.toMillis());
        JsonNode res = http.call("POST", "/v1/share/" + BigPipe.enc(cfg.group) + "/poll", body);
        List<ShareRecord> out = new ArrayList<>();
        res.path("records").forEach(r -> out.add(new ShareRecord(BigPipeRecord.fromJson(r))));
        return out;
    }

    @Override
    public void close() {
        flushAcks();
    }

    public static final class Builder {
        private String url;
        private String bootstrap;
        private String group;
        private List<String> topics = List.of();
        private String member = "java-" + UUID.randomUUID().toString().substring(0, 12);
        private int maxRecords = 100;
        private Duration lock = Duration.ofSeconds(30);
        private int maxAttempts = 5;
        private String deadLetterTopic;
        private String filter;

        public Builder bootstrap(String bootstrap) { this.bootstrap = bootstrap; return this; }
        public Builder url(String gatewayUrl) { this.url = gatewayUrl; return this; }
        public Builder group(String group) { this.group = group; return this; }
        public Builder topics(String... topics) { this.topics = List.of(topics); return this; }
        public Builder member(String member) { this.member = member; return this; }
        public Builder maxRecords(int n) { this.maxRecords = n; return this; }
        public Builder lock(Duration lock) { this.lock = lock; return this; }
        public Builder maxAttempts(int n) { this.maxAttempts = n; return this; }
        public Builder deadLetterTopic(String topic) { this.deadLetterTopic = topic; return this; }
        public Builder filter(String bpql) { this.filter = bpql; return this; }

        public ShareConsumer build() {
            if (group == null || topics.isEmpty()) throw new IllegalArgumentException("group and topics are required");
            return new ShareConsumer(this);
        }
    }
}
