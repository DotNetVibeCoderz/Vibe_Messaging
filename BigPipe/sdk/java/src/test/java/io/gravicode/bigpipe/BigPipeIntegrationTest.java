package io.gravicode.bigpipe;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Integration tests against a live node (BIGPIPE_HTTP / BIGPIPE_ADMIN, default localhost). */
class BigPipeIntegrationTest {
    private final Admin admin = new Admin();

    private static String uid(String p) {
        return p + "-" + UUID.randomUUID().toString().substring(0, 8);
    }

    @Test
    void produceAndConsumeAsGroup() {
        String topic = uid("java-orders");
        admin.createTopic(topic, 3, "local", null);
        try (Producer p = Producer.builder().bootstrap("localhost:9092").compression("lz4").build()) {
            List<CompletableFuture<RecordMetadata>> sends = new ArrayList<>();
            for (int i = 0; i < 120; i++) sends.add(p.send(topic, "k" + i, "{\"n\":" + i + "}", Map.of("lang", "java")));
            CompletableFuture.allOf(sends.toArray(CompletableFuture[]::new)).join();
            assertTrue(sends.stream().map(f -> f.join().partition()).distinct().count() > 1);
        }
        Set<Integer> seen = new HashSet<>();
        try (Consumer c = Consumer.builder().group(uid("g")).topics(topic).earliest().build()) {
            long deadline = System.currentTimeMillis() + 30_000;
            BigPipeRecord last = null;
            while (seen.size() < 120 && System.currentTimeMillis() < deadline) {
                for (BigPipeRecord r : c.poll(Duration.ofSeconds(1))) {
                    assertEquals("java", r.headers().get("lang"));
                    seen.add(r.json().get("n").asInt());
                    last = r;
                }
            }
            c.commit(last);
        }
        assertEquals(120, seen.size());
    }

    @Test
    void shareConsumerStreamAndMigration() throws Exception {
        String topic = uid("java-jobs");
        String dlq = topic + ".dlq";
        admin.createTopic(topic, 1, "diskless", null);
        List<String> streamed = new CopyOnWriteArrayList<>();
        try (BigPipeStream sub = BigPipeStream.subscribe("localhost:9092", topic, "earliest", "value() == \"job-1\"", r -> streamed.add(r.valueString()))) {
            try (Producer p = Producer.builder().build()) {
                for (int i = 0; i < 3; i++) p.send(topic, null, "job-" + i).join();
            }
            long deadline = System.currentTimeMillis() + 10_000;
            while (streamed.isEmpty() && System.currentTimeMillis() < deadline) Thread.sleep(100);
        }
        assertEquals(List.of("job-1"), streamed);

        try (ShareConsumer w = ShareConsumer.builder().group(uid("w")).topics(topic).deadLetterTopic(dlq).build()) {
            var jobs = w.poll(Duration.ofSeconds(2));
            assertEquals(3, jobs.size());
            for (var j : jobs) {
                if ("job-2".equals(j.valueString())) j.reject();
                else j.accept();
            }
        }
        List<BigPipeRecord> dead = List.of();
        for (int i = 0; i < 20 && dead.isEmpty(); i++) {
            try {
                dead = admin.browse(dlq, 10, null);
            } catch (BigPipeException ignored) {
                // DLQ topic appears asynchronously
            }
            Thread.sleep(100);
        }
        assertEquals(1, dead.size());
        assertEquals("job-2", dead.get(0).valueString());
        assertEquals("local", admin.migrate(topic, "local").get("to").asText());
    }
}
