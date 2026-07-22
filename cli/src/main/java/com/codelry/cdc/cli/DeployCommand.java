package com.codelry.cdc.cli;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import com.codelry.cdc.config.ConfigLoader;
import com.fasterxml.jackson.databind.ObjectMapper;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "deploy", description = "Validate configs and start/replace the pipeline on a running supervisor.")
public class DeployCommand implements Callable<Integer> {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Option(names = {"-c", "--connection"}, required = true, description = "Path to connection.yaml")
    Path connection;

    @Option(names = {"-m", "--mapping"}, required = true, description = "Path to mapping.yaml")
    Path mapping;

    @Option(names = {"--url"}, description = "Supervisor base URL (default: ${DEFAULT-VALUE})",
            defaultValue = "http://127.0.0.1:9405")
    String url;

    @Option(names = {"--syntax-only"}, description = "Validate locally only; do not call the supervisor")
    boolean syntaxOnly;

    @Override
    public Integer call() {
        try {
            String connectionYaml = Files.readString(connection);
            String mappingYaml = Files.readString(mapping);

            ConfigLoader loader = new ConfigLoader();
            loader.validateConnectionSyntaxYaml(connectionYaml);
            loader.validateMappingSyntaxYaml(mappingYaml);

            if (syntaxOnly) {
                System.out.println("OK: configuration is valid (not deployed)");
                return 0;
            }

            // Resolve secrets locally when possible so deploy fails fast if env is missing
            try {
                loader.loadBundleYaml(connectionYaml, mappingYaml);
            } catch (Exception e) {
                System.err.println("Warning: full secret resolution failed on client (" + e.getMessage()
                        + "); supervisor will resolve with its environment");
            }

            Map<String, String> body = new LinkedHashMap<>();
            body.put("connectionYaml", connectionYaml);
            body.put("mappingYaml", mappingYaml);

            HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
            HttpRequest request = HttpRequest.newBuilder(URI.create(trimSlash(url) + "/deploy"))
                    .timeout(Duration.ofMinutes(3))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(MAPPER.writeValueAsString(body), StandardCharsets.UTF_8))
                    .build();
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            System.out.println(response.body());
            return response.statusCode() >= 200 && response.statusCode() < 300 ? 0 : 1;
        } catch (Exception e) {
            System.err.println("deploy failed: " + e.getMessage());
            e.printStackTrace(System.err);
            return 2;
        }
    }

    private static String trimSlash(String u) {
        return u.endsWith("/") ? u.substring(0, u.length() - 1) : u;
    }
}
