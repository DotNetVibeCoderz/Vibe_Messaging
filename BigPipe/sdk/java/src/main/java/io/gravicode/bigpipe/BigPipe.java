package io.gravicode.bigpipe;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.StringJoiner;

/**
 * Entry point and shared plumbing of the BigPipe Java SDK.
 *
 * <p>The SDK talks to the BigPipe HTTP gateway (port 8082) and admin API (port 9644) with
 * {@link java.net.http.HttpClient}. For the Kafka wire protocol use the Apache Kafka Java
 * client unchanged: {@code bootstrap.servers=localhost:9092}.
 *
 * <p>Dibuat oleh Gravicode Studios dipimpin oleh Kang Fadhil.
 */
public final class BigPipe {
    static final ObjectMapper JSON = new ObjectMapper();

    private BigPipe() {
    }

    /** Gateway URL: explicit value, else host of the first bootstrap entry on port 8082, else BIGPIPE_HTTP. */
    static String gatewayUrl(String url, String bootstrap) {
        if (url != null && !url.isBlank()) return stripSlash(url);
        if (bootstrap != null && !bootstrap.isBlank()) {
            String host = bootstrap.split(",")[0].trim();
            int i = host.lastIndexOf(':');
            if (i > 0) host = host.substring(0, i);
            return "http://" + host + ":8082";
        }
        String env = System.getenv("BIGPIPE_HTTP");
        return stripSlash(env != null ? env : "http://localhost:8082");
    }

    static String stripSlash(String s) {
        return s.endsWith("/") ? s.substring(0, s.length() - 1) : s;
    }

    static String enc(String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8).replace("+", "%20");
    }

    static String query(Map<String, Object> params) {
        StringJoiner j = new StringJoiner("&", "?", "");
        j.setEmptyValue("");
        params.forEach((k, v) -> {
            if (v != null) j.add(enc(k) + "=" + enc(String.valueOf(v)));
        });
        return j.toString();
    }

    static Map<String, Object> params(Object... kv) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i + 1 < kv.length; i += 2) m.put((String) kv[i], kv[i + 1]);
        return m;
    }

    /** Minimal JSON-over-HTTP client with BigPipe error mapping. */
    static final class Http {
        final String base;
        final HttpClient client;
        final String apiKey;
        final Duration timeout;

        Http(String base, String apiKey, Duration timeout) {
            this.base = stripSlash(base);
            this.apiKey = apiKey;
            this.timeout = timeout;
            this.client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
        }

        JsonNode call(String method, String path, Object body) {
            return call(method, path, body, null);
        }

        JsonNode call(String method, String path, Object body, String rawContentType) {
            try {
                HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(base + path)).timeout(timeout);
                if (apiKey != null && !apiKey.isBlank()) b.header("Authorization", "Bearer " + apiKey);
                HttpRequest.BodyPublisher pub = HttpRequest.BodyPublishers.noBody();
                if (body != null) {
                    String payload = rawContentType != null ? body.toString() : JSON.writeValueAsString(body);
                    pub = HttpRequest.BodyPublishers.ofString(payload);
                    b.header("Content-Type", rawContentType != null ? rawContentType : "application/json");
                }
                HttpResponse<String> resp = client.send(b.method(method, pub).build(), HttpResponse.BodyHandlers.ofString());
                String text = resp.body();
                if (resp.statusCode() >= 400) throw BigPipeException.from(resp.statusCode(), text);
                return text == null || text.isEmpty() ? JSON.nullNode() : JSON.readTree(text);
            } catch (IOException e) {
                throw new BigPipeException(0, "io_error", e.getMessage(), e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new BigPipeException(0, "interrupted", e.getMessage(), e);
            }
        }
    }

    static ObjectNode encodeRecord(byte[] key, byte[] value, Map<String, String> headers, Integer partition, Long timestamp) {
        ObjectNode n = JSON.createObjectNode();
        if (key != null) {
            n.put("key", Base64.getEncoder().encodeToString(key));
            n.put("key_encoding", "base64");
        }
        if (value != null) {
            n.put("value", Base64.getEncoder().encodeToString(value));
            n.put("value_encoding", "base64");
        } else {
            n.putNull("value");
        }
        if (headers != null && !headers.isEmpty()) {
            ObjectNode h = n.putObject("headers");
            headers.forEach(h::put);
        }
        if (partition != null) n.put("partition", partition);
        if (timestamp != null) n.put("timestamp", timestamp);
        return n;
    }

    static ArrayNode array() {
        return JSON.createArrayNode();
    }
}
