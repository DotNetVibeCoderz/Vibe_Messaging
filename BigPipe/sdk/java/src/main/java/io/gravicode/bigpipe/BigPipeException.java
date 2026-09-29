package io.gravicode.bigpipe;

import com.fasterxml.jackson.databind.JsonNode;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** Error returned by BigPipe; {@link #code()} is the gateway error code (e.g. {@code unknown_topic}). */
public class BigPipeException extends RuntimeException {
    private final int status;
    private final String code;

    BigPipeException(int status, String code, String message, Throwable cause) {
        super(status + " " + code + ": " + message, cause);
        this.status = status;
        this.code = code;
    }

    static BigPipeException from(int status, String body) {
        try {
            JsonNode e = BigPipe.JSON.readTree(body).path("error");
            return new BigPipeException(status, e.path("code").asText("error"), e.path("message").asText(body), null);
        } catch (Exception ignored) {
            return new BigPipeException(status, "error", body, null);
        }
    }

    public int status() {
        return status;
    }

    public String code() {
        return code;
    }
}
