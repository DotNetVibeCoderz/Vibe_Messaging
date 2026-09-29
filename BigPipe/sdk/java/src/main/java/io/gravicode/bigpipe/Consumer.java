package io.gravicode.bigpipe;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Consumer-group member; BigPipe assigns partitions server-side (no client rebalance logic).
 *
 * <pre>{@code
 * try (var consumer = Consumer.builder().bootstrap("localhost:9092").group("billing").topics("orders").earliest().build()) {
 *     while (running) {
 *         for (BigPipeRecord r : consumer.poll(Duration.ofSeconds(1))) handle(r);
 *     }
 * }
 * }</pre>
 */
public final class Consumer implements AutoCloseable {
    private final BigPipe.Http http;
    private final Builder cfg;
    private volatile String memberId;

    private Consumer(Builder b) {
        this.cfg = b;
        this.http = new BigPipe.Http(BigPipe.gatewayUrl(b.url, b.bootstrap), null, Duration.ofSeconds(90));
        join();
    }

    public static Builder builder() {
        return new Builder();
    }

    private void join() {
        ObjectNode body = BigPipe.JSON.createObjectNode();
        body.putPOJO("topics", cfg.topics);
        body.put("auto_commit", cfg.autoCommit);
        body.put("offset_reset", cfg.offsetReset);
        if (cfg.filter != null) body.put("filter", cfg.filter);
        memberId = http.call("POST", "/v1/groups/" + BigPipe.enc(cfg.group) + "/members", body).path("member_id").asText();
    }

    public String memberId() {
        return memberId;
    }

    /** Returns records that arrived within {@code timeout} (possibly none). */
    public List<BigPipeRecord> poll(Duration timeout) {
        try {
            JsonNode res = http.call("GET", "/v1/groups/" + BigPipe.enc(cfg.group) + "/members/" + memberId + "/records"
                    + BigPipe.query(BigPipe.params("timeout_ms", timeout.toMillis(), "max_records", cfg.maxRecords)), null);
            List<BigPipeRecord> out = new ArrayList<>();
            res.path("records").forEach(r -> out.add(BigPipeRecord.fromJson(r)));
            return out;
        } catch (BigPipeException e) {
            if (e.status() == 404) { // session expired: rejoin
                join();
                return List.of();
            }
            throw e;
        }
    }

    /** Commits the position after {@code record}. */
    public void commit(BigPipeRecord record) {
        ObjectNode body = BigPipe.JSON.createObjectNode();
        ObjectNode o = body.putArray("offsets").addObject();
        o.put("topic", record.topic());
        o.put("partition", record.partition());
        o.put("offset", record.offset() + 1);
        http.call("POST", "/v1/groups/" + BigPipe.enc(cfg.group) + "/members/" + memberId + "/commit", body);
    }

    /** Commits every position returned so far. */
    public void commit() {
        http.call("POST", "/v1/groups/" + BigPipe.enc(cfg.group) + "/members/" + memberId + "/commit", BigPipe.JSON.createObjectNode());
    }

    @Override
    public void close() {
        try {
            http.call("DELETE", "/v1/groups/" + BigPipe.enc(cfg.group) + "/members/" + memberId, null);
        } catch (BigPipeException ignored) {
            // already expired
        }
    }

    public static final class Builder {
        private String url;
        private String bootstrap;
        private String group;
        private List<String> topics = List.of();
        private String offsetReset = "latest";
        private boolean autoCommit = true;
        private String filter;
        private int maxRecords = 500;

        public Builder bootstrap(String bootstrap) { this.bootstrap = bootstrap; return this; }
        public Builder url(String gatewayUrl) { this.url = gatewayUrl; return this; }
        public Builder group(String group) { this.group = group; return this; }
        public Builder topics(String... topics) { this.topics = List.of(topics); return this; }
        public Builder earliest() { this.offsetReset = "earliest"; return this; }
        public Builder latest() { this.offsetReset = "latest"; return this; }
        public Builder autoCommit(boolean enabled) { this.autoCommit = enabled; return this; }
        /** bpql filter evaluated by the broker. */
        public Builder filter(String bpql) { this.filter = bpql; return this; }
        public Builder maxRecords(int n) { this.maxRecords = n; return this; }

        public Consumer build() {
            if (group == null || topics.isEmpty()) throw new IllegalArgumentException("group and topics are required");
            return new Consumer(this);
        }
    }
}
