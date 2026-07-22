package com.codelry.cdc.config.secret;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Walks config string values and replaces {@code ${scheme:path}} references.
 */
public final class SecretInterpolator {

    private static final Pattern REF = Pattern.compile("\\$\\{([a-zA-Z][a-zA-Z0-9_]*):([^}]+)}");

    private final Map<String, SecretResolver> resolvers;

    public SecretInterpolator(List<SecretResolver> resolvers) {
        this.resolvers = new HashMap<>();
        for (SecretResolver resolver : resolvers) {
            if (resolver instanceof EnvSecretResolver) {
                this.resolvers.put("env", resolver);
            } else if (resolver instanceof VaultSecretResolver) {
                this.resolvers.put("vault", resolver);
            }
        }
        this.resolvers.putIfAbsent("env", new EnvSecretResolver());
    }

    public static SecretInterpolator defaults() {
        return new SecretInterpolator(List.of(new EnvSecretResolver(), new VaultSecretResolver()));
    }

    public String interpolate(String input) {
        if (input == null || !input.contains("${")) {
            return input;
        }
        Matcher matcher = REF.matcher(input);
        StringBuilder sb = new StringBuilder();
        while (matcher.find()) {
            String scheme = matcher.group(1);
            String path = matcher.group(2);
            SecretResolver resolver = resolvers.get(scheme);
            if (resolver == null) {
                throw new SecretResolutionException("No secret resolver registered for scheme: " + scheme);
            }
            String replacement = Matcher.quoteReplacement(resolver.resolve(scheme, path));
            matcher.appendReplacement(sb, replacement);
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    @SuppressWarnings("unchecked")
    public Object interpolateDeep(Object node) {
        if (node instanceof String s) {
            return interpolate(s);
        }
        if (node instanceof Map<?, ?> map) {
            Map<String, Object> out = new HashMap<>();
            map.forEach((k, v) -> out.put(String.valueOf(k), interpolateDeep(v)));
            return out;
        }
        if (node instanceof List<?> list) {
            return list.stream().map(this::interpolateDeep).toList();
        }
        return node;
    }
}
