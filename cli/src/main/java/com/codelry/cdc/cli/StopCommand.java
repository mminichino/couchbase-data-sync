package com.codelry.cdc.cli;

import java.nio.file.Path;
import java.util.concurrent.Callable;

import com.codelry.cdc.engine.StatusService;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "stop", description = "Request a cooperative stop of a running pipeline (writes offsets/STOP).")
public class StopCommand implements Callable<Integer> {

    @Option(names = {"-c", "--connection"}, required = true, description = "Path to connection.yaml")
    Path connection;

    @Override
    public Integer call() {
        try {
            StatusService.requestStop(connection);
            System.out.println("Stop requested. The running syncmgr process will shut down on the next batch.");
            return 0;
        } catch (Exception e) {
            System.err.println("stop failed: " + e.getMessage());
            return 2;
        }
    }
}
