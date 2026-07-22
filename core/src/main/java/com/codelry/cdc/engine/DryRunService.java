package com.codelry.cdc.engine;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codelry.cdc.config.model.ConnectionConfig;
import com.codelry.cdc.config.model.MappingConfig;
import com.codelry.cdc.sink.CouchbaseSink;
import com.codelry.cdc.sink.DeadLetterQueue;
import com.codelry.cdc.metrics.SyncMetrics;
import com.codelry.cdc.source.SourceConnectorFactory;

/**
 * Connects to source + target and introspects schemas without writing CDC data.
 */
public class DryRunService {

    private static final Logger log = LoggerFactory.getLogger(DryRunService.class);

    public DryRunReport run(ConnectionConfig connection, MappingConfig mapping) throws Exception {
        DryRunReport report = new DryRunReport();
        report.pipelineName = connection.getPipeline().getName();
        report.sourceType = connection.getSource().getType();

        // Build connector props (validates offset dirs, source type)
        Properties props = SourceConnectorFactory.build(connection);
        report.debeziumConnectorClass = props.getProperty("connector.class");
        report.tableIncludeList = props.getProperty("table.include.list");

        report.sourceTables.addAll(introspectSource(connection));

        // Mapping coverage
        for (String table : connection.getSource().getTables().getInclude()) {
            boolean mapped = mapping.getMappings().stream()
                    .anyMatch(m -> m.getSource().equalsIgnoreCase(table)
                            || table.toUpperCase(Locale.ROOT).endsWith("." + m.getSource().toUpperCase(Locale.ROOT)));
            if (mapped) {
                report.mappedTables.add(table);
            } else {
                report.unmappedTables.add(table);
            }
        }

        // Target connectivity
        SyncMetrics metrics = new SyncMetrics(connection.getPipeline().getName() + "-dryrun");
        DeadLetterQueue dlq = new DeadLetterQueue(Path.of(connection.getDeadLetter().getPath(), "dryrun"));
        try (CouchbaseSink sink = new CouchbaseSink(connection.getTarget(), dlq, metrics)) {
            sink.ping();
            report.couchbaseReachable = true;
        } finally {
            metrics.close();
        }

        report.offsetPath = connection.getOffsets().resolvedOffsetFile();
        report.schemaHistoryPath = connection.getOffsets().resolvedSchemaHistoryPath();
        report.offsetExists = Files.exists(Path.of(report.offsetPath));

        log.info("Dry-run complete for pipeline {}", report.pipelineName);
        return report;
    }

    private List<TableInfo> introspectSource(ConnectionConfig connection) throws Exception {
        ConnectionConfig.SourceConfig src = connection.getSource();
        String type = src.getType().toLowerCase(Locale.ROOT);
        String jdbcUrl = switch (type) {
            case "oracle" -> "jdbc:oracle:thin:@" + src.getHost() + ":" + src.getPort() + ":" + src.getDatabase();
            case "postgres", "postgresql" ->
                    "jdbc:postgresql://" + src.getHost() + ":" + src.getPort() + "/" + src.getDatabase();
            case "mssql", "sqlserver" ->
                    "jdbc:sqlserver://" + src.getHost() + ":" + src.getPort() + ";databaseName=" + src.getDatabase()
                            + ";encrypt=false";
            default -> throw new IllegalArgumentException("Unsupported source: " + type);
        };

        List<TableInfo> tables = new ArrayList<>();
        try (Connection jdbc = DriverManager.getConnection(jdbcUrl, src.getUser(), src.getPassword())) {
            DatabaseMetaData meta = jdbc.getMetaData();
            for (String qualified : src.getTables().getInclude()) {
                String schema = null;
                String table = qualified;
                int dot = qualified.lastIndexOf('.');
                if (dot > 0) {
                    schema = qualified.substring(0, dot);
                    table = qualified.substring(dot + 1);
                }
                TableInfo info = new TableInfo();
                info.qualifiedName = qualified;
                try (ResultSet cols = meta.getColumns(null, schema, table, null)) {
                    while (cols.next()) {
                        info.columns.add(cols.getString("COLUMN_NAME") + ":" + cols.getString("TYPE_NAME"));
                    }
                }
                try (ResultSet pks = meta.getPrimaryKeys(null, schema, table)) {
                    while (pks.next()) {
                        info.primaryKeys.add(pks.getString("COLUMN_NAME"));
                    }
                }
                tables.add(info);
            }
        }
        return tables;
    }

    public static class DryRunReport {
        public String pipelineName;
        public String sourceType;
        public String debeziumConnectorClass;
        public String tableIncludeList;
        public List<TableInfo> sourceTables = new ArrayList<>();
        public List<String> mappedTables = new ArrayList<>();
        public List<String> unmappedTables = new ArrayList<>();
        public boolean couchbaseReachable;
        public String offsetPath;
        public String schemaHistoryPath;
        public boolean offsetExists;

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("Dry-run report for ").append(pipelineName).append('\n');
            sb.append("  source: ").append(sourceType).append(" → ").append(debeziumConnectorClass).append('\n');
            sb.append("  tables: ").append(tableIncludeList).append('\n');
            sb.append("  couchbase: ").append(couchbaseReachable ? "OK" : "FAIL").append('\n');
            sb.append("  offsets: ").append(offsetPath)
                    .append(offsetExists ? " (exists)" : " (new)").append('\n');
            sb.append("  schema history: ").append(schemaHistoryPath).append('\n');
            sb.append("  mapped: ").append(mappedTables).append('\n');
            if (!unmappedTables.isEmpty()) {
                sb.append("  UNMAPPED: ").append(unmappedTables).append('\n');
            }
            for (TableInfo t : sourceTables) {
                sb.append("  ").append(t.qualifiedName)
                        .append(" pk=").append(t.primaryKeys)
                        .append(" cols=").append(t.columns.size()).append('\n');
                for (String c : t.columns) {
                    sb.append("      ").append(c).append('\n');
                }
            }
            return sb.toString();
        }
    }

    public static class TableInfo {
        public String qualifiedName;
        public List<String> columns = new ArrayList<>();
        public List<String> primaryKeys = new ArrayList<>();
    }
}
