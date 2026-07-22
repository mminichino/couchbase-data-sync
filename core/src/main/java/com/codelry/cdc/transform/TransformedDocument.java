package com.codelry.cdc.transform;

import java.util.Map;

/**
 * Result of applying a table mapping to a CDC event.
 */
public record TransformedDocument(
        String key,
        String scope,
        String collection,
        Map<String, Object> document,
        Operation operation,
        String sourceTable,
        String debeziumOp
) {
    public enum Operation {
        UPSERT,
        DELETE
    }
}
