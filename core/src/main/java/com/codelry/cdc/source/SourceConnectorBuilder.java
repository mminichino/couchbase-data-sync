package com.codelry.cdc.source;

import java.util.Properties;

import com.codelry.cdc.config.model.ConnectionConfig;

/**
 * Builds Debezium embedded-engine properties for a specific database source.
 */
public interface SourceConnectorBuilder {

    /** Source type key matching connection.yaml {@code source.type}. */
    String type();

    Properties build(ConnectionConfig connection);
}
