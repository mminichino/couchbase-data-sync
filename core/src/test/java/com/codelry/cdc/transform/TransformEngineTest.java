package com.codelry.cdc.transform;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import com.codelry.cdc.config.model.MappingConfig;

class TransformEngineTest {

    @Test
    void transformsInsertWithFieldRenameAndSoftDelete() {
        MappingConfig config = new MappingConfig();
        MappingConfig.TableMapping mapping = new MappingConfig.TableMapping();
        mapping.setSource("public.orders");
        mapping.getTarget().setScope("sales");
        mapping.getTarget().setCollection("orders");
        mapping.getKey().setTemplate("order::{id}");
        mapping.setSoftDelete(true);
        MappingConfig.FieldMapping id = new MappingConfig.FieldMapping();
        id.setSource("id");
        id.setTarget("orderId");
        MappingConfig.FieldMapping status = new MappingConfig.FieldMapping();
        status.setSource("status");
        status.setTarget("status");
        mapping.getDocument().setFields(List.of(id, status));
        mapping.getDocument().setOmit(List.of("internal_notes"));
        config.setMappings(List.of(mapping));

        TransformEngine engine = new TransformEngine(config);
        SourceRecord record = sampleRecord("c", 42L, "NEW", "secret");
        TransformedDocument doc = engine.transform(record);

        assertNotNull(doc);
        assertEquals("order::42", doc.key());
        assertEquals("sales", doc.scope());
        assertEquals("orders", doc.collection());
        assertEquals(42L, doc.document().get("orderId"));
        assertEquals("NEW", doc.document().get("status"));
        assertTrue(!doc.document().containsKey("internal_notes"));
        assertEquals(TransformedDocument.Operation.UPSERT, doc.operation());
    }

    @Test
    void softDeleteMarksDocumentInsteadOfDelete() {
        MappingConfig config = new MappingConfig();
        MappingConfig.TableMapping mapping = new MappingConfig.TableMapping();
        mapping.setSource("public.orders");
        mapping.getTarget().setScope("sales");
        mapping.getTarget().setCollection("orders");
        mapping.getKey().setTemplate("order::{id}");
        mapping.setSoftDelete(true);
        config.setMappings(List.of(mapping));

        TransformEngine engine = new TransformEngine(config);
        SourceRecord record = sampleRecord("d", 7L, "CANCELLED", null);
        TransformedDocument doc = engine.transform(record);

        assertEquals(TransformedDocument.Operation.UPSERT, doc.operation());
        assertEquals(Boolean.TRUE, doc.document().get("_deleted"));
        assertNotNull(doc.document().get("deletedAt"));
    }

    @Test
    void keyBuilderSupportsUuidAndPk() {
        KeyBuilder kb = new KeyBuilder();
        MappingConfig.KeyMapping key = new MappingConfig.KeyMapping();
        key.setTemplate("doc::{uuid4}");
        String rendered = kb.build(key, Map.of("id", 1), null);
        assertTrue(rendered.startsWith("doc::"));
        assertEquals(5 + 36, rendered.length()); // "doc::" + uuid
    }

    private static SourceRecord sampleRecord(String op, long id, String status, String notes) {
        Schema rowSchema = SchemaBuilder.struct()
                .field("id", Schema.INT64_SCHEMA)
                .field("status", Schema.STRING_SCHEMA)
                .field("internal_notes", Schema.OPTIONAL_STRING_SCHEMA)
                .optional()
                .build();
        Schema sourceSchema = SchemaBuilder.struct()
                .field("schema", Schema.STRING_SCHEMA)
                .field("table", Schema.STRING_SCHEMA)
                .build();
        Schema envelope = SchemaBuilder.struct()
                .field("before", rowSchema)
                .field("after", rowSchema)
                .field("source", sourceSchema)
                .field("op", Schema.STRING_SCHEMA)
                .build();

        Struct row = new Struct(rowSchema)
                .put("id", id)
                .put("status", status)
                .put("internal_notes", notes);
        Struct source = new Struct(sourceSchema).put("schema", "public").put("table", "orders");
        Struct value = new Struct(envelope)
                .put("before", "d".equals(op) ? row : null)
                .put("after", "d".equals(op) ? null : row)
                .put("source", source)
                .put("op", op);

        return new SourceRecord(
                Map.of("server", "test"),
                Map.of("lsn", 1L),
                "test.public.orders",
                null,
                null,
                null,
                envelope,
                value);
    }
}
