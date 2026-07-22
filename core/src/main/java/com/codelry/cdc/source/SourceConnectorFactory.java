package com.codelry.cdc.source;

import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

import com.codelry.cdc.config.ConfigValidationException;
import com.codelry.cdc.config.model.ConnectionConfig;
import com.codelry.cdc.offset.OffsetStorageConfigurator;

/**
 * Resolves the correct {@link SourceConnectorBuilder} and applies shared offset/history settings.
 */
public final class SourceConnectorFactory {

    private SourceConnectorFactory() {}

    public static Properties build(ConnectionConfig connection) {
        String type = connection.getSource().getType().toLowerCase(Locale.ROOT);
        SourceConnectorBuilder builder = findBuilder(type);
        Properties props = builder.build(connection);
        OffsetStorageConfigurator.apply(props, connection);
        return props;
    }

    public static SourceConnectorBuilder findBuilder(String type) {
        for (SourceConnectorBuilder builder : ServiceLoader.load(SourceConnectorBuilder.class)) {
            if (builder.type().equalsIgnoreCase(type)) {
                return builder;
            }
        }
        return switch (type.toLowerCase(Locale.ROOT)) {
            case "oracle" -> new OracleConnectorBuilder();
            case "postgres", "postgresql" -> new PostgresConnectorBuilder();
            case "mssql", "sqlserver" -> new MssqlConnectorBuilder();
            default -> throw new ConfigValidationException("Unsupported source type: " + type
                    + " (supported: oracle, postgres, mssql)");
        };
    }

    public static String tableIncludeList(List<String> tables) {
        return tables.stream().map(String::trim).filter(s -> !s.isEmpty()).collect(Collectors.joining(","));
    }
}
