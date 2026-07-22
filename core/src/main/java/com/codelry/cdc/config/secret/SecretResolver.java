package com.codelry.cdc.config.secret;

/**
 * Resolves secret references embedded in YAML string values.
 */
@FunctionalInterface
public interface SecretResolver {

    /**
     * Resolve a reference of the form {@code ${scheme:path}}.
     *
     * @param scheme e.g. {@code env} or {@code vault}
     * @param path   remainder after the scheme
     * @return resolved secret value
     * @throws SecretResolutionException if the secret cannot be resolved
     */
    String resolve(String scheme, String path);
}
