package io.gravicode.bigpipe;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Client for the BigPipe admin API: topics, storage-mode migration, groups, flows, message browser. */
public final class Admin {
    private final BigPipe.Http http;

    /** {@code url} null = BIGPIPE_ADMIN or http://localhost:9644; {@code apiKey} null = BIGPIPE_API_KEY. */
    public Admin(String url, String apiKey) {
        String base = url != null ? url : System.getenv().getOrDefault("BIGPIPE_ADMIN", "http://localhost:9644");
        this.http = new BigPipe.Http(base, apiKey != null ? apiKey : System.getenv("BIGPIPE_API_KEY"), Duration.ofSeconds(60));
    }

    public Admin() {
        this(null, null);
    }

    public JsonNode cluster() {
        return http.call("GET", "/v1/cluster", null);
    }

    public JsonNode topics() {
        return http.call("GET", "/v1/topics", null);
    }

    public JsonNode topic(String name) {
        return http.call("GET", "/v1/topics/" + BigPipe.enc(name), null);
    }

    /** Creates a topic; {@code mode} is local, tiered or diskless. */
    public JsonNode createTopic(String name, int partitions, String mode, Map<String, String> config) {
        ObjectNode body = BigPipe.JSON.createObjectNode();
        body.put("name", name);
        body.put("partitions", partitions);
        body.put("mode", mode);
        ObjectNode cfg = body.putObject("config");
        if (config != null) config.forEach(cfg::put);
        return http.call("POST", "/v1/topics", body);
    }

    public void deleteTopic(String name) {
        http.call("DELETE", "/v1/topics/" + BigPipe.enc(name), null);
    }

    /** Moves new data of a topic to another storage mode; existing offsets never change. */
    public JsonNode migrate(String name, String to) {
        ObjectNode body = BigPipe.JSON.createObjectNode();
        body.put("to", to);
        return http.call("POST", "/v1/topics/" + BigPipe.enc(name) + "/migrate", body);
    }

    public JsonNode group(String id) {
        return http.call("GET", "/v1/groups/" + BigPipe.enc(id), null);
    }

    public JsonNode groups() {
        return http.call("GET", "/v1/groups", null);
    }

    /** Recent records of a topic, optionally filtered with bpql. */
    public List<BigPipeRecord> browse(String topic, int limit, String filter) {
        JsonNode res = http.call("GET", "/v1/topics/" + BigPipe.enc(topic) + "/messages"
                + BigPipe.query(BigPipe.params("limit", limit, "filter", filter)), null);
        List<BigPipeRecord> out = new ArrayList<>();
        res.path("records").forEach(r -> out.add(BigPipeRecord.fromJson(r)));
        return out;
    }

    public JsonNode flows() {
        return http.call("GET", "/v1/flows", null);
    }

    /** Deploys a Kubernetes-style YAML Flow document. */
    public JsonNode deployFlowYaml(String yaml) {
        return http.call("POST", "/v1/flows", yaml, "application/yaml");
    }

    public void deleteFlow(String name) {
        http.call("DELETE", "/v1/flows/" + BigPipe.enc(name), null);
    }
}
