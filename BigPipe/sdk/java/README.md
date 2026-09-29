# BigPipe Java SDK (`io.gravicode:bigpipe-client`)

Java 17+ client for [BigPipe](https://github.com/DotNetVibeCoderz/Vibe_Messaging/tree/main/BigPipe): batching producer, server-assigned consumer groups, share groups with dead-letter topics, SSE streaming with server-side filters, and the admin API. For the Kafka wire protocol you can also use the Apache Kafka Java client unchanged (`bootstrap.servers=localhost:9092`).

*Dibuat oleh Gravicode Studios dipimpin oleh Kang Fadhil.*

```java
import io.gravicode.bigpipe.*;
import java.time.Duration;

try (var producer = Producer.builder().bootstrap("localhost:9092").compression("zstd").build()) {
    RecordMetadata m = producer.send("orders", "order-1", "{\"id\":1,\"amount\":150000}").join();
    System.out.println("written to " + m.partition() + "@" + m.offset());
}

try (var consumer = Consumer.builder().bootstrap("localhost:9092").group("billing").topics("orders").earliest().build()) {
    for (BigPipeRecord r : consumer.poll(Duration.ofSeconds(1))) {
        System.out.println(r.keyString() + " " + r.json());
        consumer.commit(r);
    }
}

try (var worker = ShareConsumer.builder().group("email-workers").topics("email-jobs").deadLetterTopic("email-jobs.dlq").build()) {
    for (var job : worker.poll(Duration.ofSeconds(2))) {
        try { send(job.record()); job.accept(); } catch (Exception e) { job.reject(); }
    }
}

var admin = new Admin("http://localhost:9644", null);
admin.createTopic("clicks", 12, "diskless", null);
admin.migrate("orders", "tiered"); // online; offsets never change
```

Build: `mvn package` (integration tests need a running `bigpiped`).

License: Apache-2.0.
