package io.gravicode.bigpipe;

import com.fasterxml.jackson.databind.JsonNode;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** Where a record was written. */
public record RecordMetadata(String topic, int partition, long offset) {
}
