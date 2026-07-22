package com.codelry.cdc.source;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import java.util.List;
import java.util.Properties;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.codelry.cdc.config.ConfigValidationException;
import com.codelry.cdc.config.model.ConnectionConfig;

class OracleConnectorBuilderTest {

    @TempDir
    Path temp;

    @Test
    void defaultsToLogMiner() {
        ConnectionConfig cfg = oracleBase();
        Properties props = new OracleConnectorBuilder().build(cfg);
        assertEquals("logminer", props.getProperty("database.connection.adapter"));
    }

    @Test
    void configuresXStreamWhenRequested() {
        ConnectionConfig cfg = oracleBase();
        cfg.getSource().setConnectionAdapter("xstream");
        cfg.getSource().setOutServerName("dbzxout");
        Properties props = new OracleConnectorBuilder().build(cfg);
        assertEquals("xstream", props.getProperty("database.connection.adapter"));
        assertEquals("dbzxout", props.getProperty("database.out.server.name"));
    }

    @Test
    void requiresOutServerForXStream() {
        ConnectionConfig cfg = oracleBase();
        cfg.getSource().setConnectionAdapter("xstream");
        assertThrows(ConfigValidationException.class, () -> new OracleConnectorBuilder().build(cfg));
    }

    private ConnectionConfig oracleBase() {
        ConnectionConfig cfg = new ConnectionConfig();
        cfg.getPipeline().setName("ora-test");
        cfg.getSource().setType("oracle");
        cfg.getSource().setHost("db");
        cfg.getSource().setPort(1521);
        cfg.getSource().setDatabase("ORCL");
        cfg.getSource().setUser("cdc");
        cfg.getSource().setPassword("pw");
        cfg.getSource().getTables().setInclude(List.of("SALES.ORDERS"));
        cfg.getOffsets().setPath(temp.toString());
        return cfg;
    }
}
