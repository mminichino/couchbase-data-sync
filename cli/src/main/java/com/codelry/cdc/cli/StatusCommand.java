package com.codelry.cdc.cli;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.Callable;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "status", description = "Show supervisor / pipeline status.")
public class StatusCommand implements Callable<Integer> {

    @Option(names = {"--url"}, description = "Supervisor base URL (default: ${DEFAULT-VALUE})",
            defaultValue = "http://127.0.0.1:9405")
    String url;

    @Override
    public Integer call() {
        try {
            HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
            HttpRequest request = HttpRequest.newBuilder(URI.create(trimSlash(url) + "/status"))
                    .timeout(Duration.ofSeconds(15))
                    .GET()
                    .build();
            HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            System.out.println(response.body());
            return response.statusCode() == 200 ? 0 : 1;
        } catch (Exception e) {
            System.err.println("status failed: " + e.getMessage());
            return 2;
        }
    }

    private static String trimSlash(String u) {
        return u.endsWith("/") ? u.substring(0, u.length() - 1) : u;
    }
}
