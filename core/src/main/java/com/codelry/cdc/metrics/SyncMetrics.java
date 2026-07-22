package com.codelry.cdc.metrics;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codelry.cdc.config.model.ConnectionConfig;
import com.sun.net.httpserver.HttpServer;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;

/**
 * Prometheus metrics exposed via a lightweight HTTP server.
 */
public class SyncMetrics implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(SyncMetrics.class);

    private final PrometheusMeterRegistry registry;
    private final Counter upserts;
    private final Counter deletes;
    private final Counter errors;
    private final Counter events;
    private final Timer transformTimer;
    private final AtomicLong lastEventEpochMs = new AtomicLong(0);
    private HttpServer server;

    public SyncMetrics(String pipelineName) {
        this.registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        registry.config().commonTags("pipeline", pipelineName);
        this.upserts = registry.counter("cdc_upserts_total");
        this.deletes = registry.counter("cdc_deletes_total");
        this.errors = registry.counter("cdc_errors_total");
        this.events = registry.counter("cdc_events_total");
        this.transformTimer = registry.timer("cdc_transform_seconds");
        registry.gauge("cdc_last_event_epoch_ms", lastEventEpochMs);
    }

    public void startHttpServer(ConnectionConfig.MetricsConfig config) throws IOException {
        if (config == null || !config.isEnabled()) {
            return;
        }
        String path = config.getPath() == null || config.getPath().isBlank() ? "/metrics" : config.getPath();
        server = HttpServer.create(new InetSocketAddress(config.getPort()), 0);
        server.createContext(path, exchange -> {
            byte[] body = scrape().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        });
        server.createContext("/health", exchange -> {
            byte[] body = "OK".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        });
        server.setExecutor(null);
        server.start();
        log.info("Prometheus metrics at http://0.0.0.0:{}{}", config.getPort(), path);
    }

    public String scrape() {
        return registry.scrape();
    }

    public void recordEvent() {
        events.increment();
        lastEventEpochMs.set(System.currentTimeMillis());
    }

    public void recordUpsert(String table) {
        upserts.increment();
        registry.counter("cdc_upserts_total", "table", safe(table)).increment();
    }

    public void recordDelete(String table) {
        deletes.increment();
        registry.counter("cdc_deletes_total", "table", safe(table)).increment();
    }

    public void recordError(String table) {
        errors.increment();
        registry.counter("cdc_errors_total", "table", safe(table)).increment();
    }

    public Timer.Sample startTransform() {
        return Timer.start(registry);
    }

    public void stopTransform(Timer.Sample sample) {
        sample.stop(transformTimer);
    }

    public MeterRegistry registry() {
        return registry;
    }

    public long lastEventEpochMs() {
        return lastEventEpochMs.get();
    }

    private static String safe(String table) {
        return table == null ? "unknown" : table;
    }

    @Override
    public void close() {
        if (server != null) {
            server.stop(0);
        }
        registry.close();
    }
}
