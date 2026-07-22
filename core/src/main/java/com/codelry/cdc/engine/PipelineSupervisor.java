package com.codelry.cdc.engine;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codelry.cdc.config.ConfigLoader;
import com.codelry.cdc.config.ConfigValidationException;
import com.codelry.cdc.config.model.ConnectionConfig;
import com.codelry.cdc.config.model.MappingConfig;
import com.codelry.cdc.metrics.SyncMetrics;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

/**
 * Idle process that accepts deploy/stop/status over HTTP and hosts at most one SyncPipeline.
 */
public final class PipelineSupervisor implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(PipelineSupervisor.class);
    private static final ObjectMapper MAPPER = new ObjectMapper().findAndRegisterModules();

    private final String bindAddress;
    private final int port;
    private final Path configDir;
    private final ExecutorService pipelineStarter = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "pipeline-starter");
        t.setDaemon(false);
        return t;
    });

    private final AtomicReference<SyncPipeline> pipeline = new AtomicReference<>();
    private final AtomicReference<String> lastError = new AtomicReference<>();
    private final Object deployLock = new Object();

    private HttpServer server;
    private final CountDownLatch shutdown = new CountDownLatch(1);

    public PipelineSupervisor(String bindAddress, int port, Path configDir) {
        this.bindAddress = bindAddress == null || bindAddress.isBlank() ? "0.0.0.0" : bindAddress;
        this.port = port;
        this.configDir = configDir;
    }

    public void start() throws IOException {
        Files.createDirectories(configDir);
        server = HttpServer.create(new InetSocketAddress(bindAddress, port), 0);
        server.createContext("/health", this::handleHealth);
        server.createContext("/status", this::handleStatus);
        server.createContext("/deploy", this::handleDeploy);
        server.createContext("/pipeline/stop", this::handleStop);
        server.createContext("/metrics", this::handleMetrics);
        server.setExecutor(Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "supervisor-http");
            t.setDaemon(true);
            return t;
        }));
        server.start();
        log.info("Supervisor listening on http://{}:{} (idle — POST /deploy to start a pipeline)",
                bindAddress, port);
    }

    public void awaitShutdown() throws InterruptedException {
        shutdown.await();
    }

    public void requestShutdown() {
        try {
            stopPipeline();
        } catch (Exception e) {
            log.warn("Error stopping pipeline on shutdown: {}", e.toString());
        }
        if (server != null) {
            server.stop(1);
        }
        pipelineStarter.shutdownNow();
        shutdown.countDown();
    }

    private void handleHealth(HttpExchange exchange) throws IOException {
        if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
            send(exchange, 405, "text/plain", "Method Not Allowed");
            return;
        }
        send(exchange, 200, "text/plain", "OK");
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
            send(exchange, 405, "application/json", "{\"error\":\"Method Not Allowed\"}");
            return;
        }
        send(exchange, 200, "application/json", MAPPER.writeValueAsString(statusMap()));
    }

    private void handleMetrics(HttpExchange exchange) throws IOException {
        if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
            send(exchange, 405, "text/plain", "Method Not Allowed");
            return;
        }
        SyncPipeline p = pipeline.get();
        SyncMetrics metrics = p == null ? null : p.metrics();
        String body = metrics == null
                ? "# no active pipeline\n"
                : metrics.scrape();
        send(exchange, 200, "text/plain; version=0.0.4; charset=utf-8", body);
    }

    private void handleDeploy(HttpExchange exchange) throws IOException {
        if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
            send(exchange, 405, "application/json", "{\"error\":\"Method Not Allowed\"}");
            return;
        }
        try {
            String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            JsonNode root = MAPPER.readTree(body);
            String connectionYaml = text(root, "connectionYaml");
            String mappingYaml = text(root, "mappingYaml");
            if (connectionYaml == null || mappingYaml == null) {
                send(exchange, 400, "application/json",
                        "{\"error\":\"connectionYaml and mappingYaml are required\"}");
                return;
            }

            ConfigLoader loader = new ConfigLoader();
            // Validate syntax first (secrets may be missing on the client; resolve on supervisor)
            loader.validateConnectionSyntaxYaml(connectionYaml);
            loader.validateMappingSyntaxYaml(mappingYaml);

            ConnectionConfig connection;
            MappingConfig mapping;
            try {
                connection = loader.loadConnectionYaml(connectionYaml);
                mapping = loader.loadMappingYaml(mappingYaml);
            } catch (ConfigValidationException e) {
                send(exchange, 400, "application/json",
                        MAPPER.writeValueAsString(Map.of("error", e.getMessage())));
                return;
            }

            persistConfigs(connectionYaml, mappingYaml);
            deploy(connection, mapping);

            Map<String, Object> resp = statusMap();
            resp.put("message", "pipeline deployed");
            send(exchange, 200, "application/json", MAPPER.writeValueAsString(resp));
        } catch (ConfigValidationException e) {
            send(exchange, 400, "application/json",
                    MAPPER.writeValueAsString(Map.of("error", e.getMessage())));
        } catch (Exception e) {
            log.error("Deploy failed", e);
            lastError.set(e.getMessage());
            send(exchange, 500, "application/json",
                    MAPPER.writeValueAsString(Map.of("error", String.valueOf(e.getMessage()))));
        }
    }

    private void handleStop(HttpExchange exchange) throws IOException {
        if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
            send(exchange, 405, "application/json", "{\"error\":\"Method Not Allowed\"}");
            return;
        }
        try {
            stopPipeline();
            Map<String, Object> resp = statusMap();
            resp.put("message", "pipeline stopped");
            send(exchange, 200, "application/json", MAPPER.writeValueAsString(resp));
        } catch (Exception e) {
            send(exchange, 500, "application/json",
                    MAPPER.writeValueAsString(Map.of("error", String.valueOf(e.getMessage()))));
        }
    }

    private void deploy(ConnectionConfig connection, MappingConfig mapping) throws Exception {
        synchronized (deployLock) {
            stopPipelineLocked();
            lastError.set(null);
            SyncPipeline next = new SyncPipeline(connection, mapping, false);
            pipeline.set(next);

            AtomicReference<Exception> startError = new AtomicReference<>();
            pipelineStarter.execute(() -> {
                try {
                    next.start();
                } catch (Exception e) {
                    log.error("Pipeline start failed", e);
                    startError.set(e);
                    lastError.set(e.getMessage());
                    pipeline.compareAndSet(next, null);
                }
            });

            // Wait until the pipeline leaves STARTING (RUNNING / WAITING_FOR_LEADER / FAILED)
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
            while (System.nanoTime() < deadline) {
                if (startError.get() != null) {
                    throw startError.get();
                }
                SyncPipeline.PipelineState state = next.state();
                if (state == SyncPipeline.PipelineState.RUNNING
                        || state == SyncPipeline.PipelineState.WAITING_FOR_LEADER
                        || state == SyncPipeline.PipelineState.FAILED
                        || state == SyncPipeline.PipelineState.STOPPED) {
                    if (state == SyncPipeline.PipelineState.FAILED) {
                        throw new IllegalStateException(lastError.get() != null
                                ? lastError.get()
                                : "pipeline failed to start");
                    }
                    return;
                }
                Thread.sleep(200);
            }
            log.info("Deploy accepted; pipeline still starting (state={})", next.state());
        }
    }

    public void stopPipeline() {
        synchronized (deployLock) {
            stopPipelineLocked();
        }
    }

    private void stopPipelineLocked() {
        SyncPipeline current = pipeline.getAndSet(null);
        if (current != null) {
            log.info("Stopping pipeline '{}'", current.connection().getPipeline().getName());
            current.stop();
        }
    }

    private Map<String, Object> statusMap() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("supervisor", "idle-or-running");
        out.put("updatedAt", Instant.now().toString());
        out.put("configDir", configDir.toAbsolutePath().toString());
        SyncPipeline p = pipeline.get();
        if (p == null) {
            out.put("pipelineState", "IDLE");
            out.put("pipeline", null);
        } else {
            out.put("pipelineState", p.state().name());
            out.put("pipeline", p.connection().getPipeline().getName());
            out.put("source", p.connection().getSource().getType());
            out.put("offsetBackend", p.connection().getOffsets().getBackend());
        }
        if (lastError.get() != null) {
            out.put("lastError", lastError.get());
        }
        return out;
    }

    private void persistConfigs(String connectionYaml, String mappingYaml) throws IOException {
        Files.createDirectories(configDir);
        Files.writeString(configDir.resolve("connection.yaml"), connectionYaml);
        Files.writeString(configDir.resolve("mapping.yaml"), mappingYaml);
    }

    private static String text(JsonNode root, String field) {
        JsonNode n = root.get(field);
        return n == null || n.isNull() ? null : n.asText();
    }

    private static void send(HttpExchange exchange, int status, String contentType, String body)
            throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", contentType);
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    @Override
    public void close() {
        requestShutdown();
    }
}
