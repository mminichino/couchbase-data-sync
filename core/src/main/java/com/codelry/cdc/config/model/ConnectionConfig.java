package com.codelry.cdc.config.model;

import java.util.ArrayList;
import java.util.List;

/**
 * Root model for connection.yaml.
 */
public class ConnectionConfig {

    private PipelineConfig pipeline = new PipelineConfig();
    private SourceConfig source = new SourceConfig();
    private TargetConfig target = new TargetConfig();
    private OffsetConfig offsets = new OffsetConfig();
    private MetricsConfig metrics = new MetricsConfig();
    private DeadLetterConfig deadLetter = new DeadLetterConfig();

    public PipelineConfig getPipeline() {
        return pipeline;
    }

    public void setPipeline(PipelineConfig pipeline) {
        this.pipeline = pipeline;
    }

    public SourceConfig getSource() {
        return source;
    }

    public void setSource(SourceConfig source) {
        this.source = source;
    }

    public TargetConfig getTarget() {
        return target;
    }

    public void setTarget(TargetConfig target) {
        this.target = target;
    }

    public OffsetConfig getOffsets() {
        return offsets;
    }

    public void setOffsets(OffsetConfig offsets) {
        this.offsets = offsets;
    }

    public MetricsConfig getMetrics() {
        return metrics;
    }

    public void setMetrics(MetricsConfig metrics) {
        this.metrics = metrics;
    }

    public DeadLetterConfig getDeadLetter() {
        return deadLetter;
    }

    public void setDeadLetter(DeadLetterConfig deadLetter) {
        this.deadLetter = deadLetter;
    }

    public static class PipelineConfig {
        private String name;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

    public static class SourceConfig {
        private String type;
        private String host;
        private int port;
        private String database;
        private String user;
        private String password;
        private String pdb;
        private TableFilter tables = new TableFilter();
        private SnapshotConfig snapshot = new SnapshotConfig();
        private String slotName;
        private String publicationName;
        private String databaseServerId;
        /** Oracle: {@code logminer} (default) or {@code xstream}. */
        private String connectionAdapter = "logminer";
        /** Required when {@code connectionAdapter=xstream}. */
        private String outServerName;

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public String getDatabase() {
            return database;
        }

        public void setDatabase(String database) {
            this.database = database;
        }

        public String getUser() {
            return user;
        }

        public void setUser(String user) {
            this.user = user;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public String getPdb() {
            return pdb;
        }

        public void setPdb(String pdb) {
            this.pdb = pdb;
        }

        public TableFilter getTables() {
            return tables;
        }

        public void setTables(TableFilter tables) {
            this.tables = tables;
        }

        public SnapshotConfig getSnapshot() {
            return snapshot;
        }

        public void setSnapshot(SnapshotConfig snapshot) {
            this.snapshot = snapshot;
        }

        public String getSlotName() {
            return slotName;
        }

        public void setSlotName(String slotName) {
            this.slotName = slotName;
        }

        public String getPublicationName() {
            return publicationName;
        }

        public void setPublicationName(String publicationName) {
            this.publicationName = publicationName;
        }

        public String getDatabaseServerId() {
            return databaseServerId;
        }

        public void setDatabaseServerId(String databaseServerId) {
            this.databaseServerId = databaseServerId;
        }

        public String getConnectionAdapter() {
            return connectionAdapter;
        }

        public void setConnectionAdapter(String connectionAdapter) {
            this.connectionAdapter = connectionAdapter;
        }

        public String getOutServerName() {
            return outServerName;
        }

        public void setOutServerName(String outServerName) {
            this.outServerName = outServerName;
        }
    }

    public static class TableFilter {
        private List<String> include = new ArrayList<>();
        private List<String> exclude = new ArrayList<>();

        public List<String> getInclude() {
            return include;
        }

        public void setInclude(List<String> include) {
            this.include = include != null ? include : new ArrayList<>();
        }

        public List<String> getExclude() {
            return exclude;
        }

        public void setExclude(List<String> exclude) {
            this.exclude = exclude != null ? exclude : new ArrayList<>();
        }
    }

    public static class SnapshotConfig {
        private String mode = "initial";

        public String getMode() {
            return mode;
        }

        public void setMode(String mode) {
            this.mode = mode;
        }
    }

    public static class TargetConfig {
        private String type = "couchbase";
        private String connectionString;
        private String username;
        private String password;
        private String bucket;
        private int batchSize = 100;
        private long flushIntervalMs = 500;
        /**
         * Durability level: {@code none} | {@code majority} |
         * {@code majorityAndPersistActive} | {@code persistToMajority}.
         */
        private String durability = "none";
        /** KV timeout applied to upsert/remove (seconds). */
        private long kvTimeoutSeconds = 10;
        /**
         * When true, create missing scopes/collections from mapping.yaml
         * (and Couchbase offset store) before the pipeline starts.
         */
        private boolean autoCreateKeyspaces = true;
        /** Max wait for a newly created collection to accept KV ops. */
        private long keyspaceReadyTimeoutSeconds = 60;

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public String getConnectionString() {
            return connectionString;
        }

        public void setConnectionString(String connectionString) {
            this.connectionString = connectionString;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public String getBucket() {
            return bucket;
        }

        public void setBucket(String bucket) {
            this.bucket = bucket;
        }

        public int getBatchSize() {
            return batchSize;
        }

        public void setBatchSize(int batchSize) {
            this.batchSize = batchSize;
        }

        public long getFlushIntervalMs() {
            return flushIntervalMs;
        }

        public void setFlushIntervalMs(long flushIntervalMs) {
            this.flushIntervalMs = flushIntervalMs;
        }

        public String getDurability() {
            return durability;
        }

        public void setDurability(String durability) {
            this.durability = durability;
        }

        public long getKvTimeoutSeconds() {
            return kvTimeoutSeconds;
        }

        public void setKvTimeoutSeconds(long kvTimeoutSeconds) {
            this.kvTimeoutSeconds = kvTimeoutSeconds;
        }

        public boolean isAutoCreateKeyspaces() {
            return autoCreateKeyspaces;
        }

        public void setAutoCreateKeyspaces(boolean autoCreateKeyspaces) {
            this.autoCreateKeyspaces = autoCreateKeyspaces;
        }

        public long getKeyspaceReadyTimeoutSeconds() {
            return keyspaceReadyTimeoutSeconds;
        }

        public void setKeyspaceReadyTimeoutSeconds(long keyspaceReadyTimeoutSeconds) {
            this.keyspaceReadyTimeoutSeconds = keyspaceReadyTimeoutSeconds;
        }
    }

    public static class OffsetConfig {
        /** {@code file} | {@code kafka} | {@code couchbase} */
        private String backend = "file";
        private String path = "/data/offsets";
        private String schemaHistoryPath;
        private KafkaOffsetConfig kafka = new KafkaOffsetConfig();
        private CouchbaseOffsetConfig couchbase = new CouchbaseOffsetConfig();
        private HaConfig ha = new HaConfig();

        public String getBackend() {
            return backend;
        }

        public void setBackend(String backend) {
            this.backend = backend;
        }

        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }

        public String getSchemaHistoryPath() {
            return schemaHistoryPath;
        }

        public void setSchemaHistoryPath(String schemaHistoryPath) {
            this.schemaHistoryPath = schemaHistoryPath;
        }

        public KafkaOffsetConfig getKafka() {
            return kafka;
        }

        public void setKafka(KafkaOffsetConfig kafka) {
            this.kafka = kafka != null ? kafka : new KafkaOffsetConfig();
        }

        public CouchbaseOffsetConfig getCouchbase() {
            return couchbase;
        }

        public void setCouchbase(CouchbaseOffsetConfig couchbase) {
            this.couchbase = couchbase != null ? couchbase : new CouchbaseOffsetConfig();
        }

        public HaConfig getHa() {
            return ha;
        }

        public void setHa(HaConfig ha) {
            this.ha = ha != null ? ha : new HaConfig();
        }

        public String resolvedSchemaHistoryPath() {
            if (schemaHistoryPath != null && !schemaHistoryPath.isBlank()) {
                return schemaHistoryPath;
            }
            return path + "/schema-history.dat";
        }

        public String resolvedOffsetFile() {
            return path + "/offsets.dat";
        }
    }

    public static class KafkaOffsetConfig {
        private String bootstrapServers;
        private String offsetTopic = "cdc-offsets";
        private String schemaHistoryTopic = "cdc-schema-history";
        private int offsetPartitions = 1;
        private short offsetReplicationFactor = 3;

        public String getBootstrapServers() {
            return bootstrapServers;
        }

        public void setBootstrapServers(String bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
        }

        public String getOffsetTopic() {
            return offsetTopic;
        }

        public void setOffsetTopic(String offsetTopic) {
            this.offsetTopic = offsetTopic;
        }

        public String getSchemaHistoryTopic() {
            return schemaHistoryTopic;
        }

        public void setSchemaHistoryTopic(String schemaHistoryTopic) {
            this.schemaHistoryTopic = schemaHistoryTopic;
        }

        public int getOffsetPartitions() {
            return offsetPartitions;
        }

        public void setOffsetPartitions(int offsetPartitions) {
            this.offsetPartitions = offsetPartitions;
        }

        public short getOffsetReplicationFactor() {
            return offsetReplicationFactor;
        }

        public void setOffsetReplicationFactor(short offsetReplicationFactor) {
            this.offsetReplicationFactor = offsetReplicationFactor;
        }
    }

    /**
     * Shared Couchbase storage for offsets/schema history (multi-replica safe).
     * When {@code useTarget=true}, credentials are taken from {@code target}.
     */
    public static class CouchbaseOffsetConfig {
        private boolean useTarget = true;
        private String connectionString;
        private String username;
        private String password;
        private String bucket;
        private String scope = "_default";
        private String collection = "cdc_state";

        public boolean isUseTarget() {
            return useTarget;
        }

        public void setUseTarget(boolean useTarget) {
            this.useTarget = useTarget;
        }

        public String getConnectionString() {
            return connectionString;
        }

        public void setConnectionString(String connectionString) {
            this.connectionString = connectionString;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public String getBucket() {
            return bucket;
        }

        public void setBucket(String bucket) {
            this.bucket = bucket;
        }

        public String getScope() {
            return scope;
        }

        public void setScope(String scope) {
            this.scope = scope;
        }

        public String getCollection() {
            return collection;
        }

        public void setCollection(String collection) {
            this.collection = collection;
        }
    }

    /**
     * Active/standby leader election so only one replica runs the Debezium engine.
     * Required for multi-replica with shared offsets (Postgres slots, Oracle LogMiner, etc.).
     */
    public static class HaConfig {
        private boolean enabled;
        private int leaseTtlSeconds = 30;
        private int renewIntervalSeconds = 10;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public int getLeaseTtlSeconds() {
            return leaseTtlSeconds;
        }

        public void setLeaseTtlSeconds(int leaseTtlSeconds) {
            this.leaseTtlSeconds = leaseTtlSeconds;
        }

        public int getRenewIntervalSeconds() {
            return renewIntervalSeconds;
        }

        public void setRenewIntervalSeconds(int renewIntervalSeconds) {
            this.renewIntervalSeconds = renewIntervalSeconds;
        }
    }

    public static class MetricsConfig {
        private boolean enabled = true;
        private int port = 9404;
        private String path = "/metrics";

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }
    }

    public static class DeadLetterConfig {
        private String path = "/data/dlq";

        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }
    }
}
