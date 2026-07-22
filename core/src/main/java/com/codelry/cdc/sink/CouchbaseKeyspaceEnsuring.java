package com.codelry.cdc.sink;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codelry.cdc.config.model.ConnectionConfig;
import com.codelry.cdc.config.model.MappingConfig;
import com.couchbase.client.core.error.CollectionExistsException;
import com.couchbase.client.core.error.CollectionNotFoundException;
import com.couchbase.client.core.error.ScopeExistsException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.kv.ExistsResult;
import com.couchbase.client.java.manager.collection.CollectionManager;
import com.couchbase.client.java.manager.collection.ScopeSpec;

/**
 * Ensures Couchbase scopes/collections from mapping (and offset store) exist,
 * then waits until they accept KV operations.
 */
public final class CouchbaseKeyspaceEnsuring {

    private static final Logger log = LoggerFactory.getLogger(CouchbaseKeyspaceEnsuring.class);
    private static final String DEFAULT_SCOPE = "_default";
    private static final String DEFAULT_COLLECTION = "_default";

    private CouchbaseKeyspaceEnsuring() {}

    public record Keyspace(String scope, String collection) {
        public Keyspace {
            scope = (scope == null || scope.isBlank()) ? DEFAULT_SCOPE : scope;
            collection = (collection == null || collection.isBlank()) ? DEFAULT_COLLECTION : collection;
        }

        @Override
        public String toString() {
            return scope + "." + collection;
        }
    }

    /**
     * Collects keyspaces required by mapping.yaml plus Couchbase offset/HA storage when configured.
     */
    public static Set<Keyspace> requiredKeyspaces(ConnectionConfig connection, MappingConfig mapping) {
        Set<Keyspace> keyspaces = new LinkedHashSet<>();
        for (MappingConfig.TableMapping m : mapping.getMappings()) {
            String scope = m.getTarget().getScope();
            String collection = m.getTarget().getCollection();
            if (collection == null || collection.isBlank()) {
                collection = defaultCollectionName(m.getSource());
            }
            keyspaces.add(new Keyspace(scope, collection));
        }

        String backend = connection.getOffsets().getBackend();
        boolean couchbaseOffsets = backend != null && "couchbase".equalsIgnoreCase(backend);
        boolean ha = connection.getOffsets().getHa().isEnabled();
        if (couchbaseOffsets || ha) {
            ConnectionConfig.CouchbaseOffsetConfig cb = connection.getOffsets().getCouchbase();
            keyspaces.add(new Keyspace(cb.getScope(), cb.getCollection()));
        }
        return keyspaces;
    }

    public static void ensure(ConnectionConfig connection, MappingConfig mapping) {
        ConnectionConfig.TargetConfig target = connection.getTarget();
        if (!target.isAutoCreateKeyspaces()) {
            log.info("target.autoCreateKeyspaces=false — skipping scope/collection creation");
            return;
        }

        Set<Keyspace> keyspaces = requiredKeyspaces(connection, mapping);
        if (keyspaces.isEmpty()) {
            return;
        }

        Duration readyTimeout = Duration.ofSeconds(Math.max(5, target.getKeyspaceReadyTimeoutSeconds()));
        log.info("Ensuring {} Couchbase keyspace(s) (readyTimeout={}): {}",
                keyspaces.size(), readyTimeout, keyspaces);

        Cluster cluster = Cluster.connect(
                target.getConnectionString(),
                ClusterOptions.clusterOptions(target.getUsername(), target.getPassword()));
        try {
            Bucket bucket = cluster.bucket(target.getBucket());
            bucket.waitUntilReady(Duration.ofSeconds(30));
            CollectionManager manager = bucket.collections();

            for (Keyspace ks : keyspaces) {
                ensureScope(manager, ks.scope());
                ensureCollection(manager, ks.scope(), ks.collection());
                waitUntilAvailable(bucket, ks, readyTimeout);
            }
            log.info("All required Couchbase keyspaces are ready");
        } finally {
            try {
                cluster.disconnect();
            } catch (Exception e) {
                log.debug("Cluster disconnect after keyspace ensure: {}", e.toString());
            }
        }
    }

    static void ensureScope(CollectionManager manager, String scope) {
        if (DEFAULT_SCOPE.equals(scope)) {
            return;
        }
        try {
            manager.createScope(scope);
            log.info("Created scope '{}'", scope);
        } catch (ScopeExistsException e) {
            log.debug("Scope '{}' already exists", scope);
        }
    }

    static void ensureCollection(CollectionManager manager, String scope, String collection) {
        if (DEFAULT_SCOPE.equals(scope) && DEFAULT_COLLECTION.equals(collection)) {
            return;
        }
        try {
            manager.createCollection(scope, collection);
            log.info("Created collection '{}.{}'", scope, collection);
        } catch (CollectionExistsException e) {
            log.debug("Collection '{}.{}' already exists", scope, collection);
        }
    }

    static void waitUntilAvailable(Bucket bucket, Keyspace ks, Duration timeout) {
        Instant deadline = Instant.now().plus(timeout);
        CollectionManager manager = bucket.collections();
        RuntimeException last = null;

        while (Instant.now().isBefore(deadline)) {
            try {
                if (!manifestContains(manager, ks)) {
                    sleep(250);
                    continue;
                }
                Collection collection = bucket.scope(ks.scope()).collection(ks.collection());
                // Probe with exists — succeeds once the collection is KV-ready
                ExistsResult ignored = collection.exists("__cdc_keyspace_ready__");
                Objects.requireNonNull(ignored);
                log.info("Keyspace {}.{} is available for KV", ks.scope(), ks.collection());
                return;
            } catch (CollectionNotFoundException e) {
                last = e;
                sleep(250);
            } catch (RuntimeException e) {
                // Manifest lag / temporary errors while collection comes online
                last = e;
                sleep(250);
            }
        }
        throw new IllegalStateException(
                "Timed out waiting for Couchbase keyspace " + ks + " after " + timeout, last);
    }

    private static boolean manifestContains(CollectionManager manager, Keyspace ks) {
        for (ScopeSpec scopeSpec : manager.getAllScopes()) {
            if (!scopeSpec.name().equals(ks.scope())) {
                continue;
            }
            return scopeSpec.collections().stream().anyMatch(c -> c.name().equals(ks.collection()));
        }
        return false;
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for Couchbase keyspace", e);
        }
    }

    static String defaultCollectionName(String source) {
        String table = source;
        int dot = source.lastIndexOf('.');
        if (dot >= 0) {
            table = source.substring(dot + 1);
        }
        return table.toLowerCase(Locale.ROOT);
    }
}
