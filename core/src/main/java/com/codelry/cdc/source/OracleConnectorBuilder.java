package com.codelry.cdc.source;

import java.util.Locale;
import java.util.Properties;

import com.codelry.cdc.config.ConfigValidationException;
import com.codelry.cdc.config.model.ConnectionConfig;

/**
 * Oracle connector properties (LogMiner by default; optional XStream).
 * <p>
 * LogMiner prerequisites: supplemental logging, LogMiner grants, optional PDB.<br>
 * XStream prerequisites: Oracle GoldenGate/XStream Out server ({@code outServerName}),
 * and a user with XStream privileges.
 */
public class OracleConnectorBuilder implements SourceConnectorBuilder {

    @Override
    public String type() {
        return "oracle";
    }

    @Override
    public Properties build(ConnectionConfig connection) {
        ConnectionConfig.SourceConfig src = connection.getSource();
        Properties props = new Properties();

        String name = connection.getPipeline().getName();
        props.setProperty("name", name);
        props.setProperty("connector.class", "io.debezium.connector.oracle.OracleConnector");
        props.setProperty("topic.prefix", sanitize(name));

        props.setProperty("database.hostname", src.getHost());
        props.setProperty("database.port", String.valueOf(src.getPort() > 0 ? src.getPort() : 1521));
        props.setProperty("database.user", src.getUser());
        props.setProperty("database.password", src.getPassword());
        props.setProperty("database.dbname", src.getDatabase());
        if (src.getPdb() != null && !src.getPdb().isBlank()) {
            props.setProperty("database.pdb.name", src.getPdb());
        }

        props.setProperty("table.include.list", SourceConnectorFactory.tableIncludeList(src.getTables().getInclude()));
        if (!src.getTables().getExclude().isEmpty()) {
            props.setProperty("table.exclude.list",
                    SourceConnectorFactory.tableIncludeList(src.getTables().getExclude()));
        }

        props.setProperty("snapshot.mode", src.getSnapshot().getMode());
        props.setProperty("schema.history.internal.store.only.captured.tables.ddl", "true");
        props.setProperty("include.schema.changes", "false");

        String adapter = src.getConnectionAdapter() == null || src.getConnectionAdapter().isBlank()
                ? "logminer"
                : src.getConnectionAdapter().toLowerCase(Locale.ROOT);
        if (!adapter.equals("logminer") && !adapter.equals("xstream")) {
            throw new ConfigValidationException(
                    "source.connectionAdapter must be 'logminer' or 'xstream', got: " + adapter);
        }
        props.setProperty("database.connection.adapter", adapter);

        if ("xstream".equals(adapter)) {
            if (src.getOutServerName() == null || src.getOutServerName().isBlank()) {
                throw new ConfigValidationException(
                        "source.outServerName is required when source.connectionAdapter=xstream");
            }
            props.setProperty("database.out.server.name", src.getOutServerName());
        }

        return props;
    }

    private static String sanitize(String name) {
        return name.replaceAll("[^a-zA-Z0-9._-]", "_");
    }
}
