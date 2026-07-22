package com.codelry.cdc.sink;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codelry.cdc.config.ConfigValidationException;
import com.codelry.cdc.config.model.ConnectionConfig;
import com.codelry.cdc.metrics.SyncMetrics;
import com.codelry.cdc.transform.TransformedDocument;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.core.msg.kv.DurabilityLevel;
import com.couchbase.client.core.retry.BestEffortRetryStrategy;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.Scope;
import com.couchbase.client.java.env.ClusterEnvironment;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.MutationResult;
import com.couchbase.client.java.kv.RemoveOptions;
import com.couchbase.client.java.kv.UpsertOptions;

/**
 * Batched Couchbase writer with durability, retries, and dead-letter fallback.
 */
public class CouchbaseSink implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(CouchbaseSink.class);

    private final Cluster cluster;
    private final Bucket bucket;
    private final int batchSize;
    private final DeadLetterQueue dlq;
    private final SyncMetrics metrics;
    private final ConcurrentLinkedQueue<TransformedDocument> queue = new ConcurrentLinkedQueue<>();
    private final ScheduledExecutorService flusher;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final UpsertOptions upsertOptions;
    private final RemoveOptions removeOptions;
    private final DurabilityLevel durabilityLevel;

    public CouchbaseSink(ConnectionConfig.TargetConfig target,
                         DeadLetterQueue dlq,
                         SyncMetrics metrics) {
        this.batchSize = Math.max(1, target.getBatchSize());
        this.dlq = dlq;
        this.metrics = metrics;
        this.durabilityLevel = parseDurability(target.getDurability());

        long kvTimeoutSec = Math.max(1, target.getKvTimeoutSeconds());
        ClusterEnvironment env = ClusterEnvironment.builder()
                .retryStrategy(BestEffortRetryStrategy.INSTANCE)
                .timeoutConfig(t -> t.kvTimeout(Duration.ofSeconds(kvTimeoutSec)))
                .build();

        this.cluster = Cluster.connect(
                target.getConnectionString(),
                ClusterOptions.clusterOptions(target.getUsername(), target.getPassword()).environment(env));
        this.bucket = cluster.bucket(target.getBucket());
        bucket.waitUntilReady(Duration.ofSeconds(30));

        UpsertOptions uo = UpsertOptions.upsertOptions().timeout(Duration.ofSeconds(kvTimeoutSec));
        RemoveOptions ro = RemoveOptions.removeOptions().timeout(Duration.ofSeconds(kvTimeoutSec));
        if (durabilityLevel != DurabilityLevel.NONE) {
            uo = uo.durability(durabilityLevel);
            ro = ro.durability(durabilityLevel);
        }
        this.upsertOptions = uo;
        this.removeOptions = ro;
        log.info("Couchbase sink durability={} kvTimeout={}s", durabilityLevel, kvTimeoutSec);

        long flushMs = Math.max(50, target.getFlushIntervalMs());
        this.flusher = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "couchbase-sink-flusher");
            t.setDaemon(true);
            return t;
        });
        flusher.scheduleAtFixedRate(this::flushSafely, flushMs, flushMs, TimeUnit.MILLISECONDS);
    }

    static DurabilityLevel parseDurability(String value) {
        if (value == null || value.isBlank()) {
            return DurabilityLevel.NONE;
        }
        return switch (value.trim().toLowerCase(Locale.ROOT)) {
            case "none" -> DurabilityLevel.NONE;
            case "majority" -> DurabilityLevel.MAJORITY;
            case "majorityandpersistactive", "majority_and_persist_to_active", "majorityandpersisttoactive" ->
                    DurabilityLevel.MAJORITY_AND_PERSIST_TO_ACTIVE;
            case "persisttomajority", "persist_to_majority" -> DurabilityLevel.PERSIST_TO_MAJORITY;
            default -> throw new ConfigValidationException(
                    "Invalid target.durability='" + value + "' (use none|majority|majorityAndPersistActive|persistToMajority)");
        };
    }

    public void write(TransformedDocument doc) {
        if (doc == null || closed.get()) {
            return;
        }
        queue.add(doc);
        if (queue.size() >= batchSize) {
            flushSafely();
        }
    }

    public void flushSafely() {
        try {
            flush();
        } catch (Exception e) {
            log.error("Unexpected flush error", e);
        }
    }

    public synchronized void flush() {
        List<TransformedDocument> batch = new ArrayList<>(batchSize);
        TransformedDocument next;
        while (batch.size() < batchSize && (next = queue.poll()) != null) {
            batch.add(next);
        }
        if (batch.isEmpty()) {
            return;
        }
        for (TransformedDocument doc : batch) {
            writeOne(doc);
        }
    }

    private void writeOne(TransformedDocument doc) {
        try {
            Collection collection = resolveCollection(doc.scope(), doc.collection());
            if (doc.operation() == TransformedDocument.Operation.DELETE) {
                try {
                    collection.remove(doc.key(), removeOptions);
                    metrics.recordDelete(doc.sourceTable());
                } catch (DocumentNotFoundException e) {
                    metrics.recordDelete(doc.sourceTable());
                }
            } else {
                JsonObject content = JsonObject.from(toJsonMap(doc.document()));
                MutationResult result = collection.upsert(doc.key(), content, upsertOptions);
                metrics.recordUpsert(doc.sourceTable());
                log.trace("Upserted {} cas={}", doc.key(), result.cas());
            }
        } catch (Exception e) {
            metrics.recordError(doc.sourceTable());
            log.warn("Couchbase write failed for key={} — sending to DLQ: {}", doc.key(), e.toString());
            dlq.offer(doc, e);
        }
    }

    private Collection resolveCollection(String scopeName, String collectionName) {
        String scope = (scopeName == null || scopeName.isBlank()) ? "_default" : scopeName;
        String coll = (collectionName == null || collectionName.isBlank()) ? "_default" : collectionName;
        Scope s = bucket.scope(scope);
        return s.collection(coll);
    }

    private static Map<String, Object> toJsonMap(Map<String, Object> document) {
        return document;
    }

    public void ping() {
        bucket.ping();
    }

    public DurabilityLevel durabilityLevel() {
        return durabilityLevel;
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        flusher.shutdown();
        try {
            flusher.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        flushSafely();
        try {
            cluster.disconnect();
        } catch (Exception e) {
            log.debug("Cluster disconnect: {}", e.toString());
        }
    }
}
