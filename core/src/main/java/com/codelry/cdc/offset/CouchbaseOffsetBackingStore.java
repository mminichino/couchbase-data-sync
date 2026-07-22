package com.codelry.cdc.offset;

import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.UpsertOptions;

/**
 * OffsetBackingStore that persists connector offsets in Couchbase KV.
 * Suitable for multi-replica / HA deployments when combined with leader election.
 * <p>
 * Config keys (via Debezium/worker originals):
 * <ul>
 *   <li>{@code offset.storage.couchbase.connectionString}</li>
 *   <li>{@code offset.storage.couchbase.username}</li>
 *   <li>{@code offset.storage.couchbase.password}</li>
 *   <li>{@code offset.storage.couchbase.bucket}</li>
 *   <li>{@code offset.storage.couchbase.scope} (default {@code _default})</li>
 *   <li>{@code offset.storage.couchbase.collection} (default {@code cdc_state})</li>
 *   <li>{@code offset.storage.couchbase.documentId} (default {@code cdc::offsets::<name>})</li>
 * </ul>
 */
public class CouchbaseOffsetBackingStore extends MemoryOffsetBackingStore {

    public static final String PROP_CONNECTION = "offset.storage.couchbase.connectionString";
    public static final String PROP_USERNAME = "offset.storage.couchbase.username";
    public static final String PROP_PASSWORD = "offset.storage.couchbase.password";
    public static final String PROP_BUCKET = "offset.storage.couchbase.bucket";
    public static final String PROP_SCOPE = "offset.storage.couchbase.scope";
    public static final String PROP_COLLECTION = "offset.storage.couchbase.collection";
    public static final String PROP_DOCUMENT_ID = "offset.storage.couchbase.documentId";

    private static final Logger log = LoggerFactory.getLogger(CouchbaseOffsetBackingStore.class);

    private Cluster cluster;
    private Collection collection;
    private String documentId;

    @Override
    public void configure(WorkerConfig config) {
        super.configure(config);
        Map<String, String> originals = config.originalsStrings();
        String connectionString = required(originals, PROP_CONNECTION);
        String username = required(originals, PROP_USERNAME);
        String password = required(originals, PROP_PASSWORD);
        String bucketName = required(originals, PROP_BUCKET);
        String scope = originals.getOrDefault(PROP_SCOPE, "_default");
        String coll = originals.getOrDefault(PROP_COLLECTION, "cdc_state");
        this.documentId = originals.getOrDefault(PROP_DOCUMENT_ID, "cdc::offsets");

        this.cluster = Cluster.connect(connectionString, ClusterOptions.clusterOptions(username, password));
        Bucket bucket = cluster.bucket(bucketName);
        bucket.waitUntilReady(java.time.Duration.ofSeconds(30));
        this.collection = bucket.scope(scope).collection(coll);
        log.info("Configured CouchbaseOffsetBackingStore documentId={} {}.{}", documentId, scope, coll);
    }

    @Override
    public synchronized void start() {
        super.start();
        load();
    }

    @Override
    public synchronized void stop() {
        super.stop();
        if (cluster != null) {
            try {
                cluster.disconnect();
            } catch (Exception e) {
                log.debug("Cluster disconnect: {}", e.toString());
            }
            cluster = null;
        }
    }

    @Override
    protected void save() {
        Map<String, String> encoded = new HashMap<>();
        for (Map.Entry<java.nio.ByteBuffer, java.nio.ByteBuffer> e : data.entrySet()) {
            String key = e.getKey() == null ? null : Base64.getEncoder().encodeToString(toArray(e.getKey()));
            String value = e.getValue() == null ? null : Base64.getEncoder().encodeToString(toArray(e.getValue()));
            encoded.put(key == null ? "" : key, value);
        }
        JsonObject doc = JsonObject.create()
                .put("type", "cdc-offsets")
                .put("updatedAt", java.time.Instant.now().toString())
                .put("entries", encoded);
        collection.upsert(documentId, doc, UpsertOptions.upsertOptions());
    }

    @SuppressWarnings("unchecked")
    private void load() {
        try {
            JsonObject doc = collection.get(documentId).contentAsObject();
            Object entries = doc.get("entries");
            if (!(entries instanceof Map<?, ?> map)) {
                return;
            }
            data = new HashMap<>();
            for (Map.Entry<?, ?> e : map.entrySet()) {
                String k = e.getKey() == null ? "" : String.valueOf(e.getKey());
                String v = e.getValue() == null ? null : String.valueOf(e.getValue());
                java.nio.ByteBuffer key = k.isEmpty() ? null : java.nio.ByteBuffer.wrap(Base64.getDecoder().decode(k));
                java.nio.ByteBuffer value = v == null || v.isEmpty()
                        ? null
                        : java.nio.ByteBuffer.wrap(Base64.getDecoder().decode(v));
                data.put(key, value);
            }
            log.info("Loaded {} offset entries from Couchbase {}", data.size(), documentId);
        } catch (DocumentNotFoundException e) {
            log.info("No existing offset document {}; starting empty", documentId);
            data = new HashMap<>();
        }
    }

    private static byte[] toArray(java.nio.ByteBuffer buf) {
        java.nio.ByteBuffer dup = buf.duplicate();
        byte[] bytes = new byte[dup.remaining()];
        dup.get(bytes);
        return bytes;
    }

    private static String required(Map<String, String> map, String key) {
        String v = map.get(key);
        if (v == null || v.isBlank()) {
            throw new IllegalArgumentException("Missing required config: " + key);
        }
        return v;
    }

    @Override
    public Set<Map<String, Object>> connectorPartitions(String connectorName) {
        return Collections.emptySet();
    }

    /** Helper used by OffsetStorageConfigurator to inject props. */
    public static void putConnectionProps(Properties props, Map<String, String> couchbaseProps) {
        couchbaseProps.forEach(props::setProperty);
    }

    public static String normalizeBackend(String backend) {
        return backend == null ? "file" : backend.toLowerCase(Locale.ROOT);
    }
}
