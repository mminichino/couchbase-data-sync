package com.codelry.cdc.source;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.List;
import java.util.Properties;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.codelry.cdc.config.model.ConnectionConfig;

class SourceConnectorFactoryTest {

    @TempDir
    Path temp;

    @Test
    void buildsPostgresPropertiesWithFileOffsets() {
        ConnectionConfig cfg = base("postgres");
        cfg.getSource().setSlotName("my_slot");
        cfg.getSource().setPublicationName("my_pub");
        Properties props = SourceConnectorFactory.build(cfg);

        assertEquals("io.debezium.connector.postgresql.PostgresConnector", props.getProperty("connector.class"));
        assertEquals("org.apache.kafka.connect.storage.FileOffsetBackingStore", props.getProperty("offset.storage"));
        assertEquals("io.debezium.storage.file.history.FileSchemaHistory", props.getProperty("schema.history.internal"));
        assertTrue(props.getProperty("offset.storage.file.filename").endsWith("offsets.dat"));
        assertEquals("pgoutput", props.getProperty("plugin.name"));
        assertEquals("my_slot", props.getProperty("slot.name"));
        assertEquals("public.orders", props.getProperty("table.include.list"));
    }

    @Test
    void buildsOracleAndMssqlConnectors() {
        Properties oracle = SourceConnectorFactory.build(base("oracle"));
        assertEquals("io.debezium.connector.oracle.OracleConnector", oracle.getProperty("connector.class"));

        Properties mssql = SourceConnectorFactory.build(base("mssql"));
        assertEquals("io.debezium.connector.sqlserver.SqlServerConnector", mssql.getProperty("connector.class"));
    }

    private ConnectionConfig base(String type) {
        ConnectionConfig cfg = new ConnectionConfig();
        cfg.getPipeline().setName("unit-test");
        cfg.getSource().setType(type);
        cfg.getSource().setHost("localhost");
        cfg.getSource().setPort(5432);
        cfg.getSource().setDatabase("db");
        cfg.getSource().setUser("u");
        cfg.getSource().setPassword("p");
        cfg.getSource().getTables().setInclude(List.of("public.orders"));
        cfg.getOffsets().setBackend("file");
        cfg.getOffsets().setPath(temp.toString());
        return cfg;
    }
}
