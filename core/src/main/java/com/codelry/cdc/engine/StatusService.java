package com.codelry.cdc.engine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import com.codelry.cdc.config.ConfigLoader;
import com.codelry.cdc.config.model.ConnectionConfig;
import com.codelry.cdc.offset.PipelineLeaderElection;

/**
 * Reads pipeline status and offset metadata.
 */
public final class StatusService {

    private StatusService() {}

    public static Map<String, String> status(Path connectionPath) throws IOException {
        ConnectionConfig connection = new ConfigLoader().loadConnection(connectionPath);
        Map<String, String> out = new LinkedHashMap<>();
        out.put("pipeline", connection.getPipeline().getName());
        out.put("source", connection.getSource().getType());
        out.put("offsetBackend", connection.getOffsets().getBackend());
        out.put("ha", String.valueOf(connection.getOffsets().getHa().isEnabled()));

        String backend = connection.getOffsets().getBackend() == null
                ? "file"
                : connection.getOffsets().getBackend().toLowerCase(Locale.ROOT);
        if ("file".equals(backend)) {
            out.put("offsetFile", connection.getOffsets().resolvedOffsetFile());
            out.put("schemaHistoryFile", connection.getOffsets().resolvedSchemaHistoryPath());
            Path offset = Path.of(connection.getOffsets().resolvedOffsetFile());
            if (Files.exists(offset)) {
                out.put("offsetSizeBytes", String.valueOf(Files.size(offset)));
                out.put("offsetLastModified", Files.getLastModifiedTime(offset).toString());
            } else {
                out.put("offsetSizeBytes", "0");
                out.put("offsetLastModified", "n/a");
            }
        } else if ("kafka".equals(backend)) {
            out.put("kafkaBootstrap", connection.getOffsets().getKafka().getBootstrapServers());
            out.put("offsetTopic", connection.getOffsets().getKafka().getOffsetTopic());
            out.put("schemaHistoryTopic", connection.getOffsets().getKafka().getSchemaHistoryTopic());
        } else if ("couchbase".equals(backend)) {
            out.put("offsetScope", connection.getOffsets().getCouchbase().getScope());
            out.put("offsetCollection", connection.getOffsets().getCouchbase().getCollection());
            out.put("offsetUseTarget", String.valueOf(connection.getOffsets().getCouchbase().isUseTarget()));
        }

        Path statusDir = Path.of(connection.getOffsets().getPath() != null
                ? connection.getOffsets().getPath()
                : "./data/offsets");
        Path statusFile = statusDir.resolve("pipeline.status");
        if (Files.exists(statusFile)) {
            Map<String, String> props = Files.readAllLines(statusFile).stream()
                    .filter(l -> l.contains("="))
                    .map(l -> l.split("=", 2))
                    .collect(Collectors.toMap(a -> a[0], a -> a[1], (a, b) -> b, LinkedHashMap::new));
            out.putAll(props);
        } else {
            out.putIfAbsent("state", "UNKNOWN");
        }

        Path stopFlag = statusDir.resolve("STOP");
        out.put("stopRequested", String.valueOf(Files.exists(stopFlag)));
        return out;
    }

    public static void requestStop(Path connectionPath) throws IOException {
        ConnectionConfig connection = new ConfigLoader().loadConnection(connectionPath);
        Path dir = Path.of(connection.getOffsets().getPath() != null
                ? connection.getOffsets().getPath()
                : "./data/offsets");
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("STOP"), java.time.Instant.now().toString());

        // Multi-replica: also write shared Couchbase stop when HA or couchbase offsets are used
        String backend = connection.getOffsets().getBackend() == null
                ? "file"
                : connection.getOffsets().getBackend().toLowerCase(Locale.ROOT);
        if (connection.getOffsets().getHa().isEnabled() || "couchbase".equals(backend)) {
            try (PipelineLeaderElection election = new PipelineLeaderElection(connection)) {
                election.requestStop();
            } catch (Exception e) {
                // Local STOP file still written; shared stop is best-effort
                System.err.println("Warning: could not write shared stop signal: " + e.getMessage());
            }
        }
    }

    public static void clearStop(Path offsetsPath) throws IOException {
        Files.deleteIfExists(Path.of(offsetsPath.toString(), "STOP"));
    }
}
