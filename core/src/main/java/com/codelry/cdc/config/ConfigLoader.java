package com.codelry.cdc.config;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.stream.Collectors;

import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;

import com.codelry.cdc.config.model.ConnectionConfig;
import com.codelry.cdc.config.model.MappingConfig;
import com.codelry.cdc.config.secret.SecretInterpolator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;

/**
 * Loads and validates connection.yaml / mapping.yaml, resolving secret refs.
 */
public final class ConfigLoader {

    private final ObjectMapper yamlMapper;
    private final ObjectMapper jsonMapper;
    private final JsonSchema connectionSchema;
    private final JsonSchema mappingSchema;
    private final SecretInterpolator interpolator;

    public ConfigLoader() {
        this(SecretInterpolator.defaults());
    }

    public ConfigLoader(SecretInterpolator interpolator) {
        this.interpolator = interpolator;
        this.yamlMapper = new ObjectMapper(new YAMLFactory()).findAndRegisterModules();
        this.jsonMapper = new ObjectMapper().findAndRegisterModules();
        JsonSchemaFactory factory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012);
        this.connectionSchema = factory.getSchema(readResource("/schemas/connection.schema.json"));
        this.mappingSchema = factory.getSchema(readResource("/schemas/mapping.schema.json"));
    }

    public ConnectionConfig loadConnection(Path path) throws IOException {
        return loadConnectionYaml(Files.readString(path), path.toString());
    }

    public MappingConfig loadMapping(Path path) throws IOException {
        return loadMappingYaml(Files.readString(path), path.toString());
    }

    public ConnectionConfig loadConnectionYaml(String yaml) throws IOException {
        return loadConnectionYaml(yaml, "connection.yaml");
    }

    public MappingConfig loadMappingYaml(String yaml) throws IOException {
        return loadMappingYaml(yaml, "mapping.yaml");
    }

    public ConnectionConfig loadConnectionYaml(String yaml, String sourceName) throws IOException {
        Object raw = loadYamlRaw(yaml);
        Object resolved = interpolator.interpolateDeep(raw);
        JsonNode tree = jsonMapper.valueToTree(resolved);
        validate(connectionSchema, tree, sourceName);
        return jsonMapper.treeToValue(tree, ConnectionConfig.class);
    }

    public MappingConfig loadMappingYaml(String yaml, String sourceName) throws IOException {
        Object raw = loadYamlRaw(yaml);
        Object resolved = interpolator.interpolateDeep(raw);
        JsonNode tree = jsonMapper.valueToTree(resolved);
        validate(mappingSchema, tree, sourceName);
        return jsonMapper.treeToValue(tree, MappingConfig.class);
    }

    /**
     * Validate without resolving secrets (useful when env vars are unavailable).
     * Secret placeholders are left as-is; schema still requires non-empty strings.
     */
    public void validateConnectionSyntax(Path path) throws IOException {
        Object raw = loadYamlRaw(Files.readString(path));
        JsonNode tree = jsonMapper.valueToTree(raw);
        validate(connectionSchema, tree, path.toString());
    }

    public void validateMappingSyntax(Path path) throws IOException {
        Object raw = loadYamlRaw(Files.readString(path));
        JsonNode tree = jsonMapper.valueToTree(raw);
        validate(mappingSchema, tree, path.toString());
    }

    public void validateConnectionSyntaxYaml(String yaml) throws IOException {
        Object raw = loadYamlRaw(yaml);
        JsonNode tree = jsonMapper.valueToTree(raw);
        validate(connectionSchema, tree, "connection.yaml");
    }

    public void validateMappingSyntaxYaml(String yaml) throws IOException {
        Object raw = loadYamlRaw(yaml);
        JsonNode tree = jsonMapper.valueToTree(raw);
        validate(mappingSchema, tree, "mapping.yaml");
    }

    public PipelineConfigBundle loadBundle(Path connectionPath, Path mappingPath) throws IOException {
        return new PipelineConfigBundle(loadConnection(connectionPath), loadMapping(mappingPath));
    }

    public PipelineConfigBundle loadBundleYaml(String connectionYaml, String mappingYaml) throws IOException {
        return new PipelineConfigBundle(loadConnectionYaml(connectionYaml), loadMappingYaml(mappingYaml));
    }

    private Object loadYamlRaw(String yaml) {
        LoaderOptions options = new LoaderOptions();
        options.setMaxAliasesForCollections(50);
        Yaml parser = new Yaml(options);
        Object loaded = parser.load(yaml);
        if (loaded == null) {
            throw new ConfigValidationException("Empty YAML");
        }
        return loaded;
    }

    private void validate(JsonSchema schema, JsonNode tree, String sourceName) {
        Set<ValidationMessage> errors = schema.validate(tree);
        if (!errors.isEmpty()) {
            String detail = errors.stream()
                    .map(ValidationMessage::getMessage)
                    .collect(Collectors.joining("; "));
            throw new ConfigValidationException("Invalid config " + sourceName + ": " + detail);
        }
    }

    private static String readResource(String classpath) {
        try (InputStream in = ConfigLoader.class.getResourceAsStream(classpath)) {
            if (in == null) {
                throw new IllegalStateException("Missing classpath resource: " + classpath);
            }
            return new String(in.readAllBytes());
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read " + classpath, e);
        }
    }

    public record PipelineConfigBundle(ConnectionConfig connection, MappingConfig mapping) {}
}
