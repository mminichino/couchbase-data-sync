package com.codelry.cdc.offset;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Properties;

import com.codelry.cdc.config.ConfigValidationException;
import com.codelry.cdc.config.model.ConnectionConfig;

/**
 * Applies offset + schema-history Debezium properties for file / kafka / couchbase backends.
 */
public final class OffsetStorageConfigurator {

    private OffsetStorageConfigurator() {}

    public static void apply(Properties props, ConnectionConfig connection) {
        ConnectionConfig.OffsetConfig offsets = connection.getOffsets();
        String backend = offsets.getBackend() == null ? "file" : offsets.getBackend().toLowerCase(Locale.ROOT);
        props.setProperty("offset.flush.interval.ms", "10000");

        switch (backend) {
            case "file" -> applyFile(props, offsets);
            case "kafka" -> applyKafka(props, connection, offsets);
            case "couchbase" -> applyCouchbase(props, connection, offsets);
            default -> throw new ConfigValidationException(
                    "Unsupported offsets.backend='" + backend + "' (supported: file, kafka, couchbase)");
        }
    }

    private static void applyFile(Properties props, ConnectionConfig.OffsetConfig offsets) {
        Path offsetDir = Path.of(offsets.getPath());
        try {
            Files.createDirectories(offsetDir);
            Path historyParent = Path.of(offsets.resolvedSchemaHistoryPath()).getParent();
            if (historyParent != null) {
                Files.createDirectories(historyParent);
            }
        } catch (Exception e) {
            throw new ConfigValidationException("Unable to create offset directories: " + offsets.getPath(), e);
        }
        props.setProperty("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore");
        props.setProperty("offset.storage.file.filename", offsets.resolvedOffsetFile());
        props.setProperty("schema.history.internal", "io.debezium.storage.file.history.FileSchemaHistory");
        props.setProperty("schema.history.internal.file.filename", offsets.resolvedSchemaHistoryPath());
    }

    private static void applyKafka(Properties props, ConnectionConfig connection, ConnectionConfig.OffsetConfig offsets) {
        ConnectionConfig.KafkaOffsetConfig kafka = offsets.getKafka();
        if (kafka.getBootstrapServers() == null || kafka.getBootstrapServers().isBlank()) {
            throw new ConfigValidationException("offsets.kafka.bootstrapServers is required when backend=kafka");
        }
        String bootstrap = kafka.getBootstrapServers();
        String name = connection.getPipeline().getName();

        props.setProperty("offset.storage", "org.apache.kafka.connect.storage.KafkaOffsetBackingStore");
        props.setProperty("offset.storage.topic", kafka.getOffsetTopic());
        props.setProperty("offset.storage.partitions", String.valueOf(kafka.getOffsetPartitions()));
        props.setProperty("offset.storage.replication.factor", String.valueOf(kafka.getOffsetReplicationFactor()));
        props.setProperty("bootstrap.servers", bootstrap);

        props.setProperty("schema.history.internal", "io.debezium.storage.kafka.history.KafkaSchemaHistory");
        props.setProperty("schema.history.internal.kafka.bootstrap.servers", bootstrap);
        props.setProperty("schema.history.internal.kafka.topic", kafka.getSchemaHistoryTopic());
        props.setProperty("schema.history.internal.name", name + "-schema-history");
    }

    private static void applyCouchbase(Properties props, ConnectionConfig connection, ConnectionConfig.OffsetConfig offsets) {
        ConnectionConfig.TargetConfig target = connection.getTarget();
        ConnectionConfig.CouchbaseOffsetConfig cb = offsets.getCouchbase();

        String connectionString = cb.isUseTarget() ? target.getConnectionString() : cb.getConnectionString();
        String username = cb.isUseTarget() ? target.getUsername() : cb.getUsername();
        String password = cb.isUseTarget() ? target.getPassword() : cb.getPassword();
        String bucket = cb.isUseTarget() ? target.getBucket() : cb.getBucket();
        if (connectionString == null || username == null || password == null || bucket == null) {
            throw new ConfigValidationException(
                    "Couchbase offset storage requires connectionString/username/password/bucket "
                            + "(set offsets.couchbase.useTarget=true or provide explicit credentials)");
        }

        String scope = cb.getScope() != null ? cb.getScope() : "_default";
        String collection = cb.getCollection() != null ? cb.getCollection() : "cdc_state";
        String pipeline = connection.getPipeline().getName();

        props.setProperty("offset.storage", CouchbaseOffsetBackingStore.class.getName());
        props.setProperty(CouchbaseOffsetBackingStore.PROP_CONNECTION, connectionString);
        props.setProperty(CouchbaseOffsetBackingStore.PROP_USERNAME, username);
        props.setProperty(CouchbaseOffsetBackingStore.PROP_PASSWORD, password);
        props.setProperty(CouchbaseOffsetBackingStore.PROP_BUCKET, bucket);
        props.setProperty(CouchbaseOffsetBackingStore.PROP_SCOPE, scope);
        props.setProperty(CouchbaseOffsetBackingStore.PROP_COLLECTION, collection);
        props.setProperty(CouchbaseOffsetBackingStore.PROP_DOCUMENT_ID, "cdc::offsets::" + pipeline);

        props.setProperty("schema.history.internal", CouchbaseSchemaHistory.class.getName());
        props.setProperty("schema.history.internal.couchbase.connectionString", connectionString);
        props.setProperty("schema.history.internal.couchbase.username", username);
        props.setProperty("schema.history.internal.couchbase.password", password);
        props.setProperty("schema.history.internal.couchbase.bucket", bucket);
        props.setProperty("schema.history.internal.couchbase.scope", scope);
        props.setProperty("schema.history.internal.couchbase.collection", collection);
        props.setProperty("schema.history.internal.couchbase.documentId", "cdc::schema-history::" + pipeline);
    }
}
