package com.codelry.cdc.transform;

import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.connect.data.Struct;

import com.codelry.cdc.config.model.MappingConfig;

/**
 * Renders document keys from templates such as {@code order::{ID}}, {@code {pk}}, {@code {uuid4}}.
 */
public class KeyBuilder {

    private static final Pattern TOKEN = Pattern.compile("\\{([^}]+)}");

    public String build(MappingConfig.KeyMapping keyMapping, Map<String, Object> columns, Object recordKey) {
        String template = keyMapping != null ? keyMapping.getTemplate() : null;
        if (template == null || template.isBlank()) {
            return primaryKeyOrUuid(columns, recordKey);
        }

        Matcher matcher = TOKEN.matcher(template);
        StringBuilder sb = new StringBuilder();
        while (matcher.find()) {
            String token = matcher.group(1).trim();
            String replacement = resolveToken(token, columns, recordKey);
            matcher.appendReplacement(sb, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    private String resolveToken(String token, Map<String, Object> columns, Object recordKey) {
        if ("uuid4".equalsIgnoreCase(token) || "uuid".equalsIgnoreCase(token)) {
            return UUID.randomUUID().toString();
        }
        if ("pk".equalsIgnoreCase(token) || "primaryKey".equalsIgnoreCase(token)) {
            return primaryKeyOrUuid(columns, recordKey);
        }
        Object value = find(columns, token);
        if (value == null) {
            throw new IllegalArgumentException("Key template references missing column: " + token);
        }
        return String.valueOf(value);
    }

    private String primaryKeyOrUuid(Map<String, Object> columns, Object recordKey) {
        if (recordKey instanceof Struct struct) {
            StringBuilder sb = new StringBuilder();
            struct.schema().fields().forEach(f -> {
                Object v = struct.get(f);
                if (v != null) {
                    if (!sb.isEmpty()) {
                        sb.append(':');
                    }
                    sb.append(v);
                }
            });
            if (!sb.isEmpty()) {
                return sb.toString();
            }
        } else if (recordKey != null) {
            return String.valueOf(recordKey);
        }
        Object id = find(columns, "ID");
        if (id == null) {
            id = find(columns, "id");
        }
        if (id != null) {
            return String.valueOf(id);
        }
        return UUID.randomUUID().toString();
    }

    private static Object find(Map<String, Object> columns, String name) {
        if (columns.containsKey(name)) {
            return columns.get(name);
        }
        for (Map.Entry<String, Object> e : columns.entrySet()) {
            if (e.getKey().equalsIgnoreCase(name)) {
                return e.getValue();
            }
        }
        return null;
    }
}
