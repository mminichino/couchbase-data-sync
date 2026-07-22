package com.codelry.cdc.source;

import java.util.Properties;

import com.codelry.cdc.config.model.ConnectionConfig;

/**
 * PostgreSQL logical decoding (pgoutput) connector properties.
 * Prerequisites: wal_level=logical, replication slot, publication.
 */
public class PostgresConnectorBuilder implements SourceConnectorBuilder {

    @Override
    public String type() {
        return "postgres";
    }

    @Override
    public Properties build(ConnectionConfig connection) {
        ConnectionConfig.SourceConfig src = connection.getSource();
        Properties props = new Properties();

        String name = connection.getPipeline().getName();
        props.setProperty("name", name);
        props.setProperty("connector.class", "io.debezium.connector.postgresql.PostgresConnector");
        props.setProperty("topic.prefix", sanitize(name));

        props.setProperty("database.hostname", src.getHost());
        props.setProperty("database.port", String.valueOf(src.getPort() > 0 ? src.getPort() : 5432));
        props.setProperty("database.user", src.getUser());
        props.setProperty("database.password", src.getPassword());
        props.setProperty("database.dbname", src.getDatabase());

        props.setProperty("table.include.list", SourceConnectorFactory.tableIncludeList(src.getTables().getInclude()));
        if (!src.getTables().getExclude().isEmpty()) {
            props.setProperty("table.exclude.list",
                    SourceConnectorFactory.tableIncludeList(src.getTables().getExclude()));
        }

        props.setProperty("plugin.name", "pgoutput");
        String slot = src.getSlotName() != null && !src.getSlotName().isBlank()
                ? src.getSlotName()
                : sanitize(name) + "_slot";
        props.setProperty("slot.name", slot);

        String publication = src.getPublicationName() != null && !src.getPublicationName().isBlank()
                ? src.getPublicationName()
                : "dbz_publication";
        props.setProperty("publication.name", publication);
        props.setProperty("publication.autocreate.mode", "filtered");

        props.setProperty("snapshot.mode", src.getSnapshot().getMode());
        props.setProperty("schema.history.internal.store.only.captured.tables.ddl", "true");
        props.setProperty("include.schema.changes", "false");
        props.setProperty("tombstones.on.delete", "false");

        return props;
    }

    private static String sanitize(String name) {
        return name.toLowerCase().replaceAll("[^a-z0-9_]", "_");
    }
}
