package com.codelry.cdc.config.secret;

/**
 * Resolves {@code ${env:VAR_NAME}} from process environment variables.
 */
public class EnvSecretResolver implements SecretResolver {

    @Override
    public String resolve(String scheme, String path) {
        if (!"env".equals(scheme)) {
            throw new SecretResolutionException("EnvSecretResolver does not support scheme: " + scheme);
        }
        String value = System.getenv(path);
        if (value == null || value.isEmpty()) {
            throw new SecretResolutionException("Environment variable not set or empty: " + path);
        }
        return value;
    }
}
