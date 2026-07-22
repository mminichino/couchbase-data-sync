package com.codelry.cdc.cli;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.Callable;

import com.codelry.cdc.engine.StatusService;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "status", description = "Show pipeline state, offsets, and lag hints.")
public class StatusCommand implements Callable<Integer> {

    @Option(names = {"-c", "--connection"}, required = true, description = "Path to connection.yaml")
    Path connection;

    @Override
    public Integer call() {
        try {
            Map<String, String> status = StatusService.status(connection);
            status.forEach((k, v) -> System.out.println(k + "=" + v));
            return 0;
        } catch (Exception e) {
            System.err.println("status failed: " + e.getMessage());
            return 2;
        }
    }
}
