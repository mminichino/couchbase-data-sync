package com.codelry.cdc.offset;

import java.net.InetAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codelry.cdc.config.model.ConnectionConfig;
import com.couchbase.client.core.error.CasMismatchException;
import com.couchbase.client.core.error.DocumentExistsException;
import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ClusterOptions;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.InsertOptions;
import com.couchbase.client.java.kv.ReplaceOptions;
import com.couchbase.client.java.kv.UpsertOptions;

/**
 * Active/standby leader election via a Couchbase lease document with TTL.
 * Only the leader should run the Debezium engine when multiple replicas share offsets.
 */
public class PipelineLeaderElection implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(PipelineLeaderElection.class);

    private final String leaseDocumentId;
    private final String instanceId;
    private final int leaseTtlSeconds;
    private final int renewIntervalSeconds;
    private final Cluster cluster;
    private final Collection collection;
    private final AtomicBoolean leader = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private ScheduledExecutorService renewer;

    public PipelineLeaderElection(ConnectionConfig connection) {
        ConnectionConfig.HaConfig ha = connection.getOffsets().getHa();
        this.leaseTtlSeconds = Math.max(5, ha.getLeaseTtlSeconds());
        this.renewIntervalSeconds = Math.max(1, Math.min(ha.getRenewIntervalSeconds(), leaseTtlSeconds / 2));
        this.instanceId = resolveInstanceId();
        this.leaseDocumentId = "cdc::lease::" + connection.getPipeline().getName();

        ConnectionConfig.TargetConfig target = connection.getTarget();
        ConnectionConfig.CouchbaseOffsetConfig cb = connection.getOffsets().getCouchbase();
        String connectionString = cb.isUseTarget() ? target.getConnectionString() : cb.getConnectionString();
        String username = cb.isUseTarget() ? target.getUsername() : cb.getUsername();
        String password = cb.isUseTarget() ? target.getPassword() : cb.getPassword();
        String bucketName = cb.isUseTarget() ? target.getBucket() : cb.getBucket();
        String scope = cb.getScope() != null ? cb.getScope() : "_default";
        String coll = cb.getCollection() != null ? cb.getCollection() : "cdc_state";

        this.cluster = Cluster.connect(connectionString, ClusterOptions.clusterOptions(username, password));
        Bucket bucket = cluster.bucket(bucketName);
        bucket.waitUntilReady(Duration.ofSeconds(30));
        this.collection = bucket.scope(scope).collection(coll);
    }

    /**
     * Blocks until this instance becomes leader (or {@link #close()} is called).
     */
    public void awaitLeadership(Supplier<Boolean> stillRunning) throws InterruptedException {
        renewer = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "cdc-leader-renew");
            t.setDaemon(true);
            return t;
        });
        renewer.scheduleAtFixedRate(this::tryAcquireOrRenew, 0, renewIntervalSeconds, TimeUnit.SECONDS);

        while (stillRunning.get() && !closed.get()) {
            if (leader.get()) {
                return;
            }
            Thread.sleep(500);
        }
    }

    public boolean isLeader() {
        return leader.get();
    }

    public String instanceId() {
        return instanceId;
    }

    private void tryAcquireOrRenew() {
        if (closed.get()) {
            return;
        }
        try {
            JsonObject body = JsonObject.create()
                    .put("leader", instanceId)
                    .put("updatedAt", Instant.now().toString());
            Duration expiry = Duration.ofSeconds(leaseTtlSeconds);

            try {
                GetResult existing = collection.get(leaseDocumentId);
                JsonObject current = existing.contentAsObject();
                String currentLeader = current.getString("leader");
                if (instanceId.equals(currentLeader) || isExpired(current)) {
                    collection.replace(leaseDocumentId, body,
                            ReplaceOptions.replaceOptions().cas(existing.cas()).expiry(expiry));
                    if (leader.compareAndSet(false, true)) {
                        log.info("Acquired pipeline leadership as {}", instanceId);
                    }
                } else {
                    if (leader.compareAndSet(true, false)) {
                        log.warn("Lost pipeline leadership to {}", currentLeader);
                    }
                }
            } catch (DocumentNotFoundException e) {
                try {
                    collection.insert(leaseDocumentId, body, InsertOptions.insertOptions().expiry(expiry));
                    leader.set(true);
                    log.info("Acquired pipeline leadership (new lease) as {}", instanceId);
                } catch (DocumentExistsException race) {
                    leader.set(false);
                }
            } catch (CasMismatchException e) {
                leader.set(false);
            }
        } catch (Exception e) {
            log.warn("Leader election tick failed: {}", e.toString());
        }
    }

    private static boolean isExpired(JsonObject current) {
        // TTL handles expiry; treat missing leader as free
        return current.getString("leader") == null || current.getString("leader").isBlank();
    }

    private static String resolveInstanceId() {
        String env = System.getenv("HOSTNAME");
        if (env != null && !env.isBlank()) {
            return env;
        }
        try {
            return InetAddress.getLocalHost().getHostName() + "-" + UUID.randomUUID().toString().substring(0, 8);
        } catch (Exception e) {
            return "syncmgr-" + UUID.randomUUID();
        }
    }

    /** Cooperative stop flag stored alongside lease (multi-replica safe). */
    public void requestStop() {
        collection.upsert("cdc::stop::" + leaseDocumentId.substring("cdc::lease::".length()),
                JsonObject.create().put("requestedAt", Instant.now().toString()),
                UpsertOptions.upsertOptions().expiry(Duration.ofHours(1)));
    }

    public boolean isStopRequested(String pipelineName) {
        try {
            collection.get("cdc::stop::" + pipelineName);
            return true;
        } catch (DocumentNotFoundException e) {
            return false;
        }
    }

    public void clearStop(String pipelineName) {
        try {
            collection.remove("cdc::stop::" + pipelineName);
        } catch (DocumentNotFoundException ignored) {
            // ignore
        }
    }

    @Override
    public void close() {
        closed.set(true);
        leader.set(false);
        if (renewer != null) {
            renewer.shutdownNow();
        }
        // Best-effort release if we hold the lease
        try {
            GetResult existing = collection.get(leaseDocumentId);
            if (instanceId.equals(existing.contentAsObject().getString("leader"))) {
                collection.remove(leaseDocumentId);
            }
        } catch (Exception ignored) {
            // ignore
        }
        try {
            cluster.disconnect();
        } catch (Exception ignored) {
            // ignore
        }
    }
}
