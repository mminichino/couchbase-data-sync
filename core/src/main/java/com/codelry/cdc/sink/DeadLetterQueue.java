package com.codelry.cdc.sink;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codelry.cdc.transform.TransformedDocument;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Appends failed writes as JSON lines to a dead-letter directory.
 */
public class DeadLetterQueue {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterQueue.class);

    private final Path directory;
    private final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();

    public DeadLetterQueue(Path directory) {
        this.directory = directory;
        try {
            Files.createDirectories(directory);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to create DLQ directory: " + directory, e);
        }
    }

    public synchronized void offer(TransformedDocument doc, Exception error) {
        Path file = directory.resolve("dlq-" + Instant.now().toString().replace(':', '-') + ".jsonl");
        // Prefer a single rolling file per day for operational simplicity
        file = directory.resolve("dlq-" + Instant.now().toString().substring(0, 10) + ".jsonl");
        try {
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("timestamp", Instant.now().toString());
            entry.put("key", doc.key());
            entry.put("scope", doc.scope());
            entry.put("collection", doc.collection());
            entry.put("operation", doc.operation().name());
            entry.put("sourceTable", doc.sourceTable());
            entry.put("document", doc.document());
            entry.put("error", error.getMessage());
            String line = mapper.writeValueAsString(entry) + "\n";
            Files.writeString(file, line, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            log.error("Failed to write DLQ entry for key={}", doc.key(), e);
        }
    }
}
