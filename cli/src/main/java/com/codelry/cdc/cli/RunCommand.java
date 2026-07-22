package com.codelry.cdc.cli;

import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codelry.cdc.engine.PipelineSupervisor;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "run", description = "Start the idle supervisor (deploy pipelines later with `syncmgr deploy`).")
public class RunCommand implements Callable<Integer> {

    private static final Logger log = LoggerFactory.getLogger(RunCommand.class);

    @Option(names = {"--bind"}, description = "Bind address (default: ${DEFAULT-VALUE})", defaultValue = "0.0.0.0")
    String bind;

    @Option(names = {"-p", "--port"}, description = "Supervisor HTTP port (default: ${DEFAULT-VALUE})", defaultValue = "9405")
    int port;

    @Option(names = {"--config-dir"}, description = "Directory for deployed YAML copies (default: ${DEFAULT-VALUE})",
            defaultValue = "./data/config")
    java.nio.file.Path configDir;

    @Override
    public Integer call() {
        try {
            PipelineSupervisor supervisor = new PipelineSupervisor(bind, port, configDir);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Shutdown hook — stopping supervisor");
                supervisor.requestShutdown();
            }, "syncmgr-shutdown"));
            supervisor.start();
            System.out.println("Supervisor ready on http://" + bind + ":" + port
                    + " — use `syncmgr deploy -c ... -m ...` to start a pipeline");
            supervisor.awaitShutdown();
            return 0;
        } catch (Exception e) {
            System.err.println("run failed: " + e.getMessage());
            e.printStackTrace(System.err);
            return 2;
        }
    }
}
