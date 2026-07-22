package com.codelry.cdc.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.codelry.cdc.config.model.MappingConfig;
import com.codelry.cdc.config.secret.EnvSecretResolver;
import com.codelry.cdc.config.secret.SecretInterpolator;
import com.codelry.cdc.config.secret.SecretResolutionException;

class ConfigLoaderTest {

    @TempDir
    Path temp;

    @Test
    void loadsValidConfigsWithSyntaxOnly() throws Exception {
        Path connection = write("""
                pipeline:
                  name: test-pipe
                source:
                  type: postgres
                  host: localhost
                  port: 5432
                  database: shop
                  user: cdc
                  password: ${env:TEST_CDC_PASSWORD}
                  tables:
                    include: ["public.orders"]
                  snapshot:
                    mode: initial
                target:
                  type: couchbase
                  connectionString: couchbase://localhost
                  username: sync
                  password: ${env:TEST_CB_PASSWORD}
                  bucket: orders
                offsets:
                  backend: file
                  path: /tmp/offsets
                """);

        Path mapping = writeMapping("""
                mappings:
                  - source: public.orders
                    target:
                      scope: sales
                      collection: orders
                    key:
                      template: "order::{id}"
                    softDelete: true
                """);

        ConfigLoader loader = new ConfigLoader();
        loader.validateConnectionSyntax(connection);
        loader.validateMappingSyntax(mapping);
        MappingConfig mc = loader.loadMapping(mapping);
        assertEquals(1, mc.getMappings().size());
        assertTrue(mc.getMappings().get(0).isSoftDelete());
    }

    @Test
    void rejectsMissingRequiredFields() throws Exception {
        Path connection = write("""
                pipeline:
                  name: bad
                source:
                  type: postgres
                """);
        ConfigLoader loader = new ConfigLoader();
        assertThrows(ConfigValidationException.class, () -> loader.validateConnectionSyntax(connection));
    }

    @Test
    void interpolatesEnvSecrets() {
        SecretInterpolator envOnly = new SecretInterpolator(List.of(new EnvSecretResolver() {
            @Override
            public String resolve(String scheme, String path) {
                if ("MY_SECRET".equals(path)) {
                    return "s3cr3t";
                }
                throw new SecretResolutionException("missing " + path);
            }
        }));
        assertEquals("s3cr3t", envOnly.interpolate("${env:MY_SECRET}"));
        @SuppressWarnings("unchecked")
        Map<String, Object> deep = (Map<String, Object>) envOnly.interpolateDeep(
                Map.of("password", "${env:MY_SECRET}"));
        assertEquals("s3cr3t", deep.get("password"));
    }

    private Path write(String yaml) throws Exception {
        Path p = temp.resolve("connection.yaml");
        Files.writeString(p, yaml);
        return p;
    }

    private Path writeMapping(String yaml) throws Exception {
        Path p = temp.resolve("mapping.yaml");
        Files.writeString(p, yaml);
        return p;
    }
}
