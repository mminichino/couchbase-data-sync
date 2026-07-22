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
        Object raw = loadYamlRaw(path);
        Object resolved = interpolator.interpolateDeep(raw);
        JsonNode tree = jsonMapper.valueToTree(resolved);
        validate(connectionSchema, tree, path);
        return jsonMapper.treeToValue(tree, ConnectionConfig.class);
    }

    public MappingConfig loadMapping(Path path) throws IOException {
        Object raw = loadYamlRaw(path);
        Object resolved = interpolator.interpolateDeep(raw);
        JsonNode tree = jsonMapper.valueToTree(resolved);
        validate(mappingSchema, tree, path);
        return jsonMapper.treeToValue(tree, MappingConfig.class);
    }

    /**
     * Validate without resolving secrets (useful when env vars are unavailable).
     * Secret placeholders are left as-is; schema still requires non-empty strings.
     */
    public void validateConnectionSyntax(Path path) throws IOException {
        Object raw = loadYamlRaw(path);
        JsonNode tree = jsonMapper.valueToTree(raw);
        validate(connectionSchema, tree, path);
    }

    public void validateMappingSyntax(Path path) throws IOException {
        Object raw = loadYamlRaw(path);
        JsonNode tree = jsonMapper.valueToTree(raw);
        validate(mappingSchema, tree, path);
    }

    public PipelineConfigBundle loadBundle(Path connectionPath, Path mappingPath) throws IOException {
        return new PipelineConfigBundle(loadConnection(connectionPath), loadMapping(mappingPath));
    }

    private Object loadYamlRaw(Path path) throws IOException {
        LoaderOptions options = new LoaderOptions();
        options.setMaxAliasesForCollections(50);
        Yaml yaml = new Yaml(options);
        try (InputStream in = Files.newInputStream(path)) {
            Object loaded = yaml.load(in);
            if (loaded == null) {
                throw new ConfigValidationException("Empty YAML file: " + path);
            }
            return loaded;
        }
    }

    private void validate(JsonSchema schema, JsonNode tree, Path path) {
        Set<ValidationMessage> errors = schema.validate(tree);
        if (!errors.isEmpty()) {
            String detail = errors.stream()
                    .map(ValidationMessage::getMessage)
                    .collect(Collectors.joining("; "));
            throw new ConfigValidationException("Invalid config " + path + ": " + detail);
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
