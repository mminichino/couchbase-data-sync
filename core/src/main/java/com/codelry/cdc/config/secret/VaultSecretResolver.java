package com.codelry.cdc.config.secret;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Resolves {@code ${vault:secret/data/path#field}} via HashiCorp Vault KV v2 HTTP API.
 * <p>
 * Requires {@code VAULT_ADDR} and {@code VAULT_TOKEN} (or AppRole via env).
 * Path format: {@code mount/data/secret/path#field} or {@code secret/data/myapp#password}.
 */
public class VaultSecretResolver implements SecretResolver {

    private final HttpClient httpClient;
    private final ObjectMapper mapper;
    private final String vaultAddr;
    private final String vaultToken;

    public VaultSecretResolver() {
        this(
                System.getenv().getOrDefault("VAULT_ADDR", "http://127.0.0.1:8200"),
                System.getenv("VAULT_TOKEN")
        );
    }

    public VaultSecretResolver(String vaultAddr, String vaultToken) {
        this.vaultAddr = vaultAddr;
        this.vaultToken = vaultToken;
        this.httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
        this.mapper = new ObjectMapper();
    }

    @Override
    public String resolve(String scheme, String path) {
        if (!"vault".equals(scheme)) {
            throw new SecretResolutionException("VaultSecretResolver does not support scheme: " + scheme);
        }
        if (vaultToken == null || vaultToken.isBlank()) {
            throw new SecretResolutionException("VAULT_TOKEN is required to resolve vault secrets");
        }

        String secretPath = path;
        String field = "value";
        int hash = path.indexOf('#');
        if (hash >= 0) {
            secretPath = path.substring(0, hash);
            field = path.substring(hash + 1);
        }

        String url = vaultAddr.replaceAll("/$", "") + "/v1/" + secretPath;
        HttpRequest request = HttpRequest.newBuilder(URI.create(url))
                .timeout(Duration.ofSeconds(15))
                .header("X-Vault-Token", vaultToken)
                .GET()
                .build();

        try {
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() >= 300) {
                throw new SecretResolutionException(
                        "Vault returned HTTP " + response.statusCode() + " for " + secretPath);
            }
            JsonNode root = mapper.readTree(response.body());
            JsonNode data = root.path("data");
            // KV v2 nests under data.data
            if (data.has("data")) {
                data = data.get("data");
            }
            JsonNode value = data.get(field);
            if (value == null || value.isNull()) {
                throw new SecretResolutionException("Vault secret field not found: " + field + " in " + secretPath);
            }
            return value.asText();
        } catch (IOException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new SecretResolutionException("Failed to resolve vault secret: " + path, e);
        }
    }

    /** Convenience for tests / DI. */
    public Map<String, String> describe() {
        return Map.of("vaultAddr", vaultAddr, "tokenConfigured", String.valueOf(vaultToken != null));
    }
}
