package com.codelry.cdc.cli;

import java.nio.file.Path;
import java.util.concurrent.Callable;

import com.codelry.cdc.config.ConfigLoader;
import com.codelry.cdc.engine.DryRunService;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "dry-run", description = "Connect to source + Couchbase and introspect schemas (no CDC writes).")
public class DryRunCommand implements Callable<Integer> {

    @Option(names = {"-c", "--connection"}, required = true, description = "Path to connection.yaml")
    Path connection;

    @Option(names = {"-m", "--mapping"}, required = true, description = "Path to mapping.yaml")
    Path mapping;

    @Override
    public Integer call() {
        try {
            var bundle = new ConfigLoader().loadBundle(connection, mapping);
            DryRunService.DryRunReport report = new DryRunService().run(bundle.connection(), bundle.mapping());
            System.out.print(report);
            return report.unmappedTables.isEmpty() && report.couchbaseReachable ? 0 : 1;
        } catch (Exception e) {
            System.err.println("dry-run failed: " + e.getMessage());
            e.printStackTrace(System.err);
            return 2;
        }
    }
}
