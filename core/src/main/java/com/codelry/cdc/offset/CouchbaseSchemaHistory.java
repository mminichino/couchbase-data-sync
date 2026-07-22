package com.codelry.cdc.offset;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.UpsertOptions;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.document.DocumentReader;
import io.debezium.document.DocumentWriter;
import io.debezium.relational.history.AbstractFileBasedSchemaHistory;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.relational.history.SchemaHistory;
import io.debezium.relational.history.SchemaHistoryException;
import io.debezium.relational.history.SchemaHistoryListener;
import io.debezium.util.Collect;

/**
 * Schema history stored as a JSON document in Couchbase (array of history lines).
 */
public class CouchbaseSchemaHistory extends AbstractFileBasedSchemaHistory {

    private static final String P = SchemaHistory.CONFIGURATION_FIELD_PREFIX_STRING + "couchbase.";

    public static final Field CONNECTION = Field.create(P + "connectionString").required();
    public static final Field USERNAME = Field.create(P + "username").required();
    public static final Field PASSWORD = Field.create(P + "password").required();
    public static final Field BUCKET = Field.create(P + "bucket").required();
    public static final Field SCOPE = Field.create(P + "scope").withDefault("_default");
    public static final Field COLLECTION_NAME = Field.create(P + "collection").withDefault("cdc_state");
    public static final Field DOCUMENT_ID = Field.create(P + "documentId").withDefault("cdc::schema-history");

    public static Collection<Field> ALL_FIELDS = Collect.arrayListOf(
            CONNECTION, USERNAME, PASSWORD, BUCKET, SCOPE, COLLECTION_NAME, DOCUMENT_ID);

    private final DocumentWriter writer = DocumentWriter.defaultWriter();
    private final DocumentReader reader = DocumentReader.defaultReader();

    private Cluster cluster;
    private com.couchbase.client.java.Collection kvCollection;
    private String documentId;

    @Override
    public void configure(Configuration config, HistoryRecordComparator comparator,
                          SchemaHistoryListener listener, boolean useCatalogBeforeSchema) {
        if (!config.validateAndRecord(ALL_FIELDS, logger::error)) {
            throw new DebeziumException("Error configuring " + getClass().getSimpleName());
        }
        super.configure(config, comparator, listener, useCatalogBeforeSchema);
        String connectionString = config.getString(CONNECTION);
        String username = config.getString(USERNAME);
        String password = config.getString(PASSWORD);
        String bucketName = config.getString(BUCKET);
        String scope = config.getString(SCOPE);
        String coll = config.getString(COLLECTION_NAME);
        this.documentId = config.getString(DOCUMENT_ID);

        this.cluster = Cluster.connect(connectionString, ClusterOptions.clusterOptions(username, password));
        Bucket bucket = cluster.bucket(bucketName);
        bucket.waitUntilReady(Duration.ofSeconds(30));
        this.kvCollection = bucket.scope(scope).collection(coll);
    }

    @Override
    protected void doStart() {
        try {
            JsonObject doc = kvCollection.get(documentId).contentAsObject();
            JsonArray lines = doc.getArray("lines");
            if (lines == null) {
                return;
            }
            for (int i = 0; i < lines.size(); i++) {
                String line = lines.getString(i);
                if (line != null && !line.isEmpty()) {
                    records.add(new HistoryRecord(reader.read(line)));
                }
            }
            logger.info("Loaded {} schema history records from Couchbase {}", records.size(), documentId);
        } catch (DocumentNotFoundException e) {
            logger.info("No schema history document {}; starting empty", documentId);
        } catch (Exception e) {
            throw new SchemaHistoryException("Unable to load Couchbase schema history", e);
        }
    }

    @Override
    protected void doStoreRecord(HistoryRecord record) {
        try {
            records.add(record);
            List<String> lines = new ArrayList<>(records.size());
            for (HistoryRecord r : records) {
                lines.add(writer.write(r.document()));
            }
            JsonObject doc = JsonObject.create()
                    .put("type", "cdc-schema-history")
                    .put("updatedAt", java.time.Instant.now().toString())
                    .put("lines", lines);
            kvCollection.upsert(documentId, doc, UpsertOptions.upsertOptions());
        } catch (Exception e) {
            throw new SchemaHistoryException("Unable to store schema history to Couchbase", e);
        }
    }

    @Override
    public boolean storageExists() {
        try {
            kvCollection.get(documentId);
            return true;
        } catch (DocumentNotFoundException e) {
            return false;
        }
    }

    @Override
    public void initializeStorage() {
        JsonObject doc = JsonObject.create()
                .put("type", "cdc-schema-history")
                .put("lines", JsonArray.create());
        kvCollection.upsert(documentId, doc, UpsertOptions.upsertOptions());
    }

    @Override
    protected void doStop() {
        if (cluster != null) {
            try {
                cluster.disconnect();
            } catch (Exception ignored) {
                // ignore
            }
            cluster = null;
        }
    }

    @Override
    public String toString() {
        return "couchbase:" + documentId;
    }
}
