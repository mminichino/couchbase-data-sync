package com.codelry.cdc.source;

import java.util.Properties;

import com.codelry.cdc.config.model.ConnectionConfig;

/**
 * SQL Server CDC connector properties.
 * Prerequisites: CDC enabled on database and each captured table.
 */
public class MssqlConnectorBuilder implements SourceConnectorBuilder {

    @Override
    public String type() {
        return "mssql";
    }

    @Override
    public Properties build(ConnectionConfig connection) {
        ConnectionConfig.SourceConfig src = connection.getSource();
        Properties props = new Properties();

        String name = connection.getPipeline().getName();
        props.setProperty("name", name);
        props.setProperty("connector.class", "io.debezium.connector.sqlserver.SqlServerConnector");
        props.setProperty("topic.prefix", sanitize(name));

        props.setProperty("database.hostname", src.getHost());
        props.setProperty("database.port", String.valueOf(src.getPort() > 0 ? src.getPort() : 1433));
        props.setProperty("database.user", src.getUser());
        props.setProperty("database.password", src.getPassword());
        props.setProperty("database.names", src.getDatabase());

        props.setProperty("table.include.list", SourceConnectorFactory.tableIncludeList(src.getTables().getInclude()));
        if (!src.getTables().getExclude().isEmpty()) {
            props.setProperty("table.exclude.list",
                    SourceConnectorFactory.tableIncludeList(src.getTables().getExclude()));
        }

        props.setProperty("snapshot.mode", src.getSnapshot().getMode());
        props.setProperty("schema.history.internal.store.only.captured.tables.ddl", "true");
        props.setProperty("include.schema.changes", "false");

        return props;
    }

    private static String sanitize(String name) {
        return name.replaceAll("[^a-zA-Z0-9._-]", "_");
    }
}
