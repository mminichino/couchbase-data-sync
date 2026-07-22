package com.codelry.cdc.offset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.List;
import java.util.Properties;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.codelry.cdc.config.ConfigValidationException;
import com.codelry.cdc.config.model.ConnectionConfig;

class OffsetStorageConfiguratorTest {

    @TempDir
    Path temp;

    @Test
    void configuresFileBackend() {
        ConnectionConfig cfg = base();
        cfg.getOffsets().setBackend("file");
        cfg.getOffsets().setPath(temp.toString());
        Properties props = new Properties();
        OffsetStorageConfigurator.apply(props, cfg);
        assertEquals("org.apache.kafka.connect.storage.FileOffsetBackingStore", props.getProperty("offset.storage"));
        assertTrue(props.getProperty("offset.storage.file.filename").endsWith("offsets.dat"));
        assertEquals("io.debezium.storage.file.history.FileSchemaHistory",
                props.getProperty("schema.history.internal"));
    }

    @Test
    void configuresKafkaBackend() {
        ConnectionConfig cfg = base();
        cfg.getOffsets().setBackend("kafka");
        cfg.getOffsets().getKafka().setBootstrapServers("kafka:9092");
        cfg.getOffsets().getKafka().setOffsetTopic("offsets");
        cfg.getOffsets().getKafka().setSchemaHistoryTopic("history");
        Properties props = new Properties();
        OffsetStorageConfigurator.apply(props, cfg);
        assertEquals("org.apache.kafka.connect.storage.KafkaOffsetBackingStore", props.getProperty("offset.storage"));
        assertEquals("kafka:9092", props.getProperty("bootstrap.servers"));
        assertEquals("io.debezium.storage.kafka.history.KafkaSchemaHistory",
                props.getProperty("schema.history.internal"));
        assertEquals("kafka:9092", props.getProperty("schema.history.internal.kafka.bootstrap.servers"));
    }

    @Test
    void configuresCouchbaseBackendFromTarget() {
        ConnectionConfig cfg = base();
        cfg.getOffsets().setBackend("couchbase");
        cfg.getOffsets().getCouchbase().setUseTarget(true);
        cfg.getOffsets().getCouchbase().setScope("sys");
        cfg.getOffsets().getCouchbase().setCollection("cdc_state");
        Properties props = new Properties();
        OffsetStorageConfigurator.apply(props, cfg);
        assertEquals(CouchbaseOffsetBackingStore.class.getName(), props.getProperty("offset.storage"));
        assertEquals(CouchbaseSchemaHistory.class.getName(), props.getProperty("schema.history.internal"));
        assertEquals("couchbase://localhost", props.getProperty(CouchbaseOffsetBackingStore.PROP_CONNECTION));
        assertEquals("cdc::offsets::unit-test", props.getProperty(CouchbaseOffsetBackingStore.PROP_DOCUMENT_ID));
    }

    @Test
    void rejectsUnknownBackend() {
        ConnectionConfig cfg = base();
        cfg.getOffsets().setBackend("redis");
        assertThrows(ConfigValidationException.class,
                () -> OffsetStorageConfigurator.apply(new Properties(), cfg));
    }

    private ConnectionConfig base() {
        ConnectionConfig cfg = new ConnectionConfig();
        cfg.getPipeline().setName("unit-test");
        cfg.getSource().setType("postgres");
        cfg.getSource().setHost("localhost");
        cfg.getSource().setPort(5432);
        cfg.getSource().setDatabase("db");
        cfg.getSource().setUser("u");
        cfg.getSource().setPassword("p");
        cfg.getSource().getTables().setInclude(List.of("public.orders"));
        cfg.getTarget().setConnectionString("couchbase://localhost");
        cfg.getTarget().setUsername("sync");
        cfg.getTarget().setPassword("pw");
        cfg.getTarget().setBucket("orders");
        cfg.getOffsets().setPath(temp.toString());
        return cfg;
    }
}
