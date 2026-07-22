package com.codelry.cdc.engine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codelry.cdc.config.ConfigLoader;
import com.codelry.cdc.config.ConfigValidationException;
import com.codelry.cdc.config.model.ConnectionConfig;
import com.codelry.cdc.config.model.MappingConfig;
import com.codelry.cdc.metrics.SyncMetrics;
import com.codelry.cdc.offset.PipelineLeaderElection;
import com.codelry.cdc.sink.CouchbaseKeyspaceEnsuring;
import com.codelry.cdc.sink.CouchbaseSink;
import com.codelry.cdc.sink.DeadLetterQueue;
import com.codelry.cdc.source.SourceConnectorFactory;
import com.codelry.cdc.transform.TransformEngine;
import com.codelry.cdc.transform.TransformedDocument;

import io.debezium.embedded.Connect;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.RecordChangeEvent;
import io.debezium.engine.format.ChangeEventFormat;
import io.micrometer.core.instrument.Timer;

/**
 * Wires Debezium embedded engine → transform → Couchbase sink.
 */
public class SyncPipeline implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(SyncPipeline.class);

    private final ConnectionConfig connection;
    private final MappingConfig mapping;
    private final Path statusFile;
    private final AtomicReference<PipelineState> state = new AtomicReference<>(PipelineState.STOPPED);
    private final AtomicBoolean stopRequested = new AtomicBoolean(false);
    private final CountDownLatch terminated = new CountDownLatch(1);

    private SyncMetrics metrics;
    private CouchbaseSink sink;
    private DebeziumEngine<?> engine;
    private ExecutorService engineExecutor;
    private PipelineLeaderElection leaderElection;

    public SyncPipeline(ConnectionConfig connection, MappingConfig mapping) {
        this.connection = connection;
        this.mapping = mapping;
        String statusDir = connection.getOffsets().getPath() != null
                ? connection.getOffsets().getPath()
                : "./data/offsets";
        this.statusFile = Path.of(statusDir, "pipeline.status");
    }

    public static SyncPipeline fromFiles(Path connectionPath, Path mappingPath) throws IOException {
        ConfigLoader.PipelineConfigBundle bundle = new ConfigLoader().loadBundle(connectionPath, mappingPath);
        return new SyncPipeline(bundle.connection(), bundle.mapping());
    }

    public void start() throws Exception {
        if (!state.compareAndSet(PipelineState.STOPPED, PipelineState.STARTING)
                && !state.compareAndSet(PipelineState.FAILED, PipelineState.STARTING)) {
            throw new IllegalStateException("Pipeline already " + state.get());
        }
        writeStatus();

        validateHaConfig();

        metrics = new SyncMetrics(connection.getPipeline().getName());
        metrics.startHttpServer(connection.getMetrics());

        // Create mapping / offset scopes+collections before HA lease or sink writes
        CouchbaseKeyspaceEnsuring.ensure(connection, mapping);

        if (connection.getOffsets().getHa().isEnabled()) {
            state.set(PipelineState.WAITING_FOR_LEADER);
            writeStatus();
            leaderElection = new PipelineLeaderElection(connection);
            log.info("HA enabled — waiting for leadership (instance={})", leaderElection.instanceId());
            leaderElection.awaitLeadership(() -> !stopRequested.get());
            if (stopRequested.get()) {
                cleanupResources();
                state.set(PipelineState.STOPPED);
                writeStatus();
                terminated.countDown();
                return;
            }
            leaderElection.clearStop(connection.getPipeline().getName());
            log.info("Leadership acquired — starting engine");
        }

        DeadLetterQueue dlq = new DeadLetterQueue(Path.of(connection.getDeadLetter().getPath()));
        sink = new CouchbaseSink(connection.getTarget(), dlq, metrics);

        TransformEngine transform = new TransformEngine(mapping);
        Properties props = SourceConnectorFactory.build(connection);
        Path stopFlag = Path.of(statusFile.getParent().toString(), "STOP");

        engine = DebeziumEngine.create(ChangeEventFormat.of(Connect.class))
                .using(props)
                .notifying((records, committer) -> {
                    for (RecordChangeEvent<SourceRecord> event : records) {
                        handle(transform, event.record());
                        committer.markProcessed(event);
                    }
                    committer.markBatchFinished();
                    if (shouldStop(stopFlag)) {
                        log.info("Stop requested — closing Debezium engine");
                        stopRequested.set(true);
                        Thread closer = new Thread(() -> {
                            try {
                                engine.close();
                            } catch (Exception e) {
                                log.warn("Error closing engine: {}", e.toString());
                            }
                        }, "debezium-closer");
                        closer.setDaemon(true);
                        closer.start();
                    }
                })
                .using((success, message, error) -> {
                    if (!success) {
                        log.error("Debezium engine stopped unsuccessfully: {}", message, error);
                        state.set(PipelineState.FAILED);
                    } else {
                        log.info("Debezium engine stopped: {}", message);
                        if (state.get() != PipelineState.FAILED) {
                            state.set(PipelineState.STOPPED);
                        }
                    }
                    cleanupResources();
                    writeStatus();
                    terminated.countDown();
                })
                .build();

        engineExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "debezium-engine-" + connection.getPipeline().getName());
            t.setDaemon(false);
            return t;
        });

        Files.deleteIfExists(stopFlag);
        state.set(PipelineState.RUNNING);
        writeStatus();
        engineExecutor.submit(engine);
        log.info("Pipeline '{}' started (offsets.backend={})",
                connection.getPipeline().getName(), connection.getOffsets().getBackend());
    }

    private void validateHaConfig() {
        if (!connection.getOffsets().getHa().isEnabled()) {
            return;
        }
        String backend = connection.getOffsets().getBackend() == null
                ? "file"
                : connection.getOffsets().getBackend().toLowerCase(Locale.ROOT);
        if ("file".equals(backend)) {
            throw new ConfigValidationException(
                    "offsets.ha.enabled requires offsets.backend=kafka or couchbase "
                            + "(file offsets are not multi-replica safe)");
        }
    }

    private boolean shouldStop(Path stopFlag) {
        if (stopRequested.get() || Files.exists(stopFlag)) {
            return true;
        }
        if (leaderElection != null) {
            if (!leaderElection.isLeader()) {
                log.warn("Lost leadership — stopping engine");
                return true;
            }
            return leaderElection.isStopRequested(connection.getPipeline().getName());
        }
        return false;
    }

    private void handle(TransformEngine transform, SourceRecord record) {
        if (record == null) {
            return;
        }
        metrics.recordEvent();
        Timer.Sample sample = metrics.startTransform();
        try {
            TransformedDocument doc = transform.transform(record);
            if (doc != null) {
                sink.write(doc);
            }
        } catch (Exception e) {
            metrics.recordError(record.topic());
            log.warn("Transform failed for topic={}: {}", record.topic(), e.toString());
        } finally {
            metrics.stopTransform(sample);
        }
    }

    public void await() throws InterruptedException {
        terminated.await();
    }

    public void stop() {
        stopRequested.set(true);
        state.set(PipelineState.STOPPING);
        writeStatus();
        if (leaderElection != null) {
            try {
                leaderElection.requestStop();
            } catch (Exception e) {
                log.debug("Shared stop signal: {}", e.toString());
            }
        }
        if (engine != null) {
            try {
                engine.close();
            } catch (IOException e) {
                log.warn("Error closing engine: {}", e.toString());
            }
            try {
                terminated.await(60, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        } else {
            // Stopped before engine started (e.g. while waiting for HA leadership)
            cleanupResources();
            state.set(PipelineState.STOPPED);
            writeStatus();
            terminated.countDown();
        }
        if (engineExecutor != null) {
            engineExecutor.shutdownNow();
        }
    }

    private void cleanupResources() {
        if (sink != null) {
            sink.close();
            sink = null;
        }
        if (metrics != null) {
            metrics.close();
            metrics = null;
        }
        if (leaderElection != null) {
            leaderElection.close();
            leaderElection = null;
        }
        try {
            Files.deleteIfExists(Path.of(statusFile.getParent().toString(), "STOP"));
        } catch (IOException ignored) {
            // ignore
        }
    }

    public PipelineState state() {
        return state.get();
    }

    public ConnectionConfig connection() {
        return connection;
    }

    public Path statusFile() {
        return statusFile;
    }

    private void writeStatus() {
        try {
            Files.createDirectories(statusFile.getParent());
            String body = "name=" + connection.getPipeline().getName() + "\n"
                    + "state=" + state.get() + "\n"
                    + "updatedAt=" + java.time.Instant.now() + "\n"
                    + "offsetBackend=" + connection.getOffsets().getBackend() + "\n"
                    + "ha=" + connection.getOffsets().getHa().isEnabled() + "\n"
                    + "offsetFile=" + connection.getOffsets().resolvedOffsetFile() + "\n";
            Files.writeString(statusFile, body);
        } catch (IOException e) {
            log.debug("Unable to write status file: {}", e.toString());
        }
    }

    @Override
    public void close() {
        stop();
    }

    public enum PipelineState {
        STOPPED, STARTING, WAITING_FOR_LEADER, RUNNING, STOPPING, FAILED
    }
}
