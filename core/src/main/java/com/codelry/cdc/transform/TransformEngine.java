package com.codelry.cdc.transform;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import com.codelry.cdc.config.model.MappingConfig;

/**
 * Applies mapping.yaml rules to a Debezium change event.
 */
public class TransformEngine {

    private static final DateTimeFormatter ISO = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

    private final Map<String, MappingConfig.TableMapping> byTable;
    private final KeyBuilder keyBuilder = new KeyBuilder();

    public TransformEngine(MappingConfig mappingConfig) {
        this.byTable = mappingConfig.getMappings().stream()
                .collect(Collectors.toMap(
                        m -> normalizeTable(m.getSource()),
                        m -> m,
                        (a, b) -> a,
                        LinkedHashMap::new));
    }

    public TransformedDocument transform(SourceRecord record) {
        if (record == null || record.value() == null) {
            return null;
        }
        if (!(record.value() instanceof Struct envelope)) {
            return null;
        }

        String op = stringField(envelope, "op");
        Struct source = envelope.getStruct("source");
        String tableKey = resolveTableKey(source, record);
        MappingConfig.TableMapping mapping = lookup(tableKey);
        if (mapping == null) {
            return null; // unmapped table — skip
        }

        boolean isDelete = "d".equals(op);
        Struct row = isDelete ? envelope.getStruct("before") : envelope.getStruct("after");
        if (row == null && isDelete) {
            row = envelope.getStruct("before");
        }
        if (row == null) {
            return null;
        }

        Map<String, Object> columns = structToMap(row);
        Map<String, Object> document = buildDocument(columns, mapping);
        String key = keyBuilder.build(mapping.getKey(), columns, record.key());

        String scope = mapping.getTarget().getScope();
        String collection = mapping.getTarget().getCollection();
        if (collection == null || collection.isBlank()) {
            collection = defaultCollectionName(mapping.getSource());
        }

        TransformedDocument.Operation operation;
        if (isDelete) {
            if (mapping.isSoftDelete()) {
                document.put("_deleted", true);
                document.put("deletedAt", Instant.now().atOffset(ZoneOffset.UTC).format(ISO));
                operation = TransformedDocument.Operation.UPSERT;
            } else {
                operation = TransformedDocument.Operation.DELETE;
            }
        } else {
            operation = TransformedDocument.Operation.UPSERT;
        }

        return new TransformedDocument(key, scope, collection, document, operation, tableKey, op);
    }

    private MappingConfig.TableMapping lookup(String tableKey) {
        MappingConfig.TableMapping exact = byTable.get(normalizeTable(tableKey));
        if (exact != null) {
            return exact;
        }
        // allow schema-only or table-only matches
        for (Map.Entry<String, MappingConfig.TableMapping> e : byTable.entrySet()) {
            if (normalizeTable(tableKey).endsWith("." + e.getKey()) || e.getKey().endsWith("." + normalizeTable(tableKey))) {
                return e.getValue();
            }
        }
        return null;
    }

    private Map<String, Object> buildDocument(Map<String, Object> columns, MappingConfig.TableMapping mapping) {
        MappingConfig.DocumentMapping doc = mapping.getDocument();
        Set<String> omit = doc.getOmit().stream()
                .map(s -> s.toUpperCase(Locale.ROOT))
                .collect(Collectors.toSet());

        Map<String, Object> out = new LinkedHashMap<>();
        List<MappingConfig.FieldMapping> fields = doc.getFields();

        if (fields == null || fields.isEmpty()) {
            columns.forEach((k, v) -> {
                if (!omit.contains(k.toUpperCase(Locale.ROOT))) {
                    out.put(k, v);
                }
            });
            return out;
        }

        Map<String, String> renames = new HashMap<>();
        Map<String, String> types = new HashMap<>();
        for (MappingConfig.FieldMapping f : fields) {
            String target = f.getTarget() != null && !f.getTarget().isBlank() ? f.getTarget() : f.getSource();
            renames.put(f.getSource().toUpperCase(Locale.ROOT), target);
            if (f.getType() != null) {
                types.put(f.getSource().toUpperCase(Locale.ROOT), f.getType());
            }
        }

        // Emit explicitly mapped fields first (preserving order), then remaining columns
        for (MappingConfig.FieldMapping f : fields) {
            String src = f.getSource();
            Object value = findColumn(columns, src);
            if (omit.contains(src.toUpperCase(Locale.ROOT))) {
                continue;
            }
            String target = renames.get(src.toUpperCase(Locale.ROOT));
            out.put(target, coerce(value, types.get(src.toUpperCase(Locale.ROOT))));
        }

        for (Map.Entry<String, Object> e : columns.entrySet()) {
            String upper = e.getKey().toUpperCase(Locale.ROOT);
            if (omit.contains(upper) || renames.containsKey(upper)) {
                continue;
            }
            out.put(e.getKey(), e.getValue());
        }
        return out;
    }

    private static Object findColumn(Map<String, Object> columns, String name) {
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

    private static Object coerce(Object value, String type) {
        if (value == null || type == null || type.isBlank()) {
            return value;
        }
        return switch (type) {
            case "string" -> String.valueOf(value);
            case "number" -> value instanceof Number n ? n : Double.valueOf(String.valueOf(value));
            case "boolean" -> value instanceof Boolean b ? b : Boolean.valueOf(String.valueOf(value));
            case "iso8601" -> toIso8601(value);
            case "epochMillis" -> toEpochMillis(value);
            default -> value;
        };
    }

    private static String toIso8601(Object value) {
        if (value instanceof java.util.Date d) {
            return d.toInstant().atOffset(ZoneOffset.UTC).format(ISO);
        }
        if (value instanceof Instant i) {
            return i.atOffset(ZoneOffset.UTC).format(ISO);
        }
        if (value instanceof Long l) {
            // Debezium often emits epoch micros/millis depending on converter
            long millis = l > 1_000_000_000_000_000L ? l / 1000 : l;
            if (millis > 1_000_000_000_000L) {
                // likely micros already handled; treat as millis if 13 digits
            }
            if (String.valueOf(l).length() > 13) {
                millis = l / 1000;
            }
            return Instant.ofEpochMilli(millis).atOffset(ZoneOffset.UTC).format(ISO);
        }
        return String.valueOf(value);
    }

    private static long toEpochMillis(Object value) {
        if (value instanceof Number n) {
            long v = n.longValue();
            return String.valueOf(v).length() > 13 ? v / 1000 : v;
        }
        if (value instanceof java.util.Date d) {
            return d.getTime();
        }
        if (value instanceof Instant i) {
            return i.toEpochMilli();
        }
        return Long.parseLong(String.valueOf(value));
    }

    private static Map<String, Object> structToMap(Struct struct) {
        Map<String, Object> map = new LinkedHashMap<>();
        for (Field field : struct.schema().fields()) {
            Object v = struct.get(field);
            if (v instanceof Struct nested) {
                map.put(field.name(), structToMap(nested));
            } else {
                map.put(field.name(), v);
            }
        }
        return map;
    }

    private static String resolveTableKey(Struct source, SourceRecord record) {
        if (source != null) {
            String schema = stringField(source, "schema");
            String table = stringField(source, "table");
            if (table != null) {
                if (schema != null && !schema.isBlank()) {
                    return schema + "." + table;
                }
                String db = stringField(source, "db");
                if (db != null && !db.isBlank()) {
                    return db + "." + table;
                }
                return table;
            }
        }
        // topic: prefix.schema.table
        String topic = record.topic();
        if (topic != null) {
            int idx = topic.indexOf('.');
            if (idx > 0 && idx < topic.length() - 1) {
                return topic.substring(idx + 1);
            }
        }
        return topic;
    }

    private static String stringField(Struct struct, String field) {
        try {
            Object v = struct.get(field);
            return v == null ? null : String.valueOf(v);
        } catch (Exception e) {
            return null;
        }
    }

    private static String normalizeTable(String name) {
        return Objects.requireNonNullElse(name, "").trim().toUpperCase(Locale.ROOT);
    }

    private static String defaultCollectionName(String source) {
        String table = source;
        int dot = source.lastIndexOf('.');
        if (dot >= 0) {
            table = source.substring(dot + 1);
        }
        return table.toLowerCase(Locale.ROOT);
    }
}
