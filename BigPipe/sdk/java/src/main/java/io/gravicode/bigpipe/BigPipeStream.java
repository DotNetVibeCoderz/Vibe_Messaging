package io.gravicode.bigpipe;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

/**
 * Server-Sent Events subscription with an optional bpql filter evaluated by the broker.
 *
 * <pre>{@code
 * try (var sub = BigPipeStream.subscribe("localhost:9092", "payments", "latest",
 *         "header(\"region\") == \"ID\"", r -> System.out.println(r.json()))) {
 *     Thread.sleep(60_000);
 * }
 * }</pre>
 */
public final class BigPipeStream implements AutoCloseable {
    private final Thread thread;
    private volatile InputStream body;
    private volatile boolean closed;

    private BigPipeStream(String url, Consumer<BigPipeRecord> handler, Consumer<Throwable> onError) {
        thread = new Thread(() -> run(url, handler, onError), "bigpipe-sse");
        thread.setDaemon(true);
        thread.start();
    }

    /**
     * Subscribes to a topic. {@code bootstrap} may be a Kafka bootstrap (host:9092) or a gateway URL.
     * {@code from} is "earliest", "latest" or an offset.
     */
    public static BigPipeStream subscribe(String bootstrap, String topic, String from, String filter, Consumer<BigPipeRecord> handler) {
        return subscribe(bootstrap, topic, from, filter, handler, e -> { });
    }

    public static BigPipeStream subscribe(String bootstrap, String topic, String from, String filter,
                                          Consumer<BigPipeRecord> handler, Consumer<Throwable> onError) {
        String base = bootstrap != null && bootstrap.startsWith("http") ? BigPipe.stripSlash(bootstrap) : BigPipe.gatewayUrl(null, bootstrap);
        String url = base + "/v1/topics/" + BigPipe.enc(topic) + "/stream"
                + BigPipe.query(BigPipe.params("from", from == null ? "latest" : from, "filter", filter));
        return new BigPipeStream(url, handler, onError);
    }

    private void run(String url, Consumer<BigPipeRecord> handler, Consumer<Throwable> onError) {
        try {
            HttpRequest req = HttpRequest.newBuilder(URI.create(url)).header("Accept", "text/event-stream").GET().build();
            HttpResponse<InputStream> resp = HttpClient.newHttpClient().send(req, HttpResponse.BodyHandlers.ofInputStream());
            body = resp.body();
            if (resp.statusCode() >= 400) {
                onError.accept(BigPipeException.from(resp.statusCode(), new String(body.readAllBytes(), StandardCharsets.UTF_8)));
                return;
            }
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(body, StandardCharsets.UTF_8))) {
                String event = null;
                String line;
                while (!closed && (line = reader.readLine()) != null) {
                    if (line.startsWith("event:")) event = line.substring(6).trim();
                    else if (line.startsWith("data:") && "record".equals(event))
                        handler.accept(BigPipeRecord.fromJson(BigPipe.JSON.readTree(line.substring(5))));
                    else if (line.isEmpty()) event = null;
                }
            }
        } catch (Exception e) {
            if (!closed) onError.accept(e);
        }
    }

    @Override
    public void close() {
        closed = true;
        try {
            if (body != null) body.close();
        } catch (Exception ignored) {
            // closing
        }
        thread.interrupt();
    }
}
