package com.codelry.cdc.cli;

import java.nio.file.Path;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codelry.cdc.engine.SyncPipeline;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "run", description = "Start the CDC pipeline and block until stopped.")
public class RunCommand implements Callable<Integer> {

    private static final Logger log = LoggerFactory.getLogger(RunCommand.class);

    @Option(names = {"-c", "--connection"}, required = true, description = "Path to connection.yaml")
    Path connection;

    @Option(names = {"-m", "--mapping"}, required = true, description = "Path to mapping.yaml")
    Path mapping;

    @Override
    public Integer call() {
        try {
            SyncPipeline pipeline = SyncPipeline.fromFiles(connection, mapping);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Shutdown hook — stopping pipeline");
                pipeline.stop();
            }, "syncmgr-shutdown"));
            pipeline.start();
            pipeline.await();
            return pipeline.state() == SyncPipeline.PipelineState.FAILED ? 1 : 0;
        } catch (Exception e) {
            System.err.println("run failed: " + e.getMessage());
            e.printStackTrace(System.err);
            return 2;
        }
    }
}
