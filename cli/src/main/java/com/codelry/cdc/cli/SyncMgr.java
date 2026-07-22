package com.codelry.cdc.cli;

import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(
        name = "syncmgr",
        mixinStandardHelpOptions = true,
        version = "syncmgr 0.1.0-SNAPSHOT",
        description = "Manage Couchbase CDC sync pipelines (Oracle / Postgres / MS SQL → Couchbase).",
        subcommands = {
                ValidateCommand.class,
                DryRunCommand.class,
                RunCommand.class,
                DeployCommand.class,
                StatusCommand.class,
                StopCommand.class,
                CommandLine.HelpCommand.class
        }
)
public class SyncMgr implements Runnable {

    @Override
    public void run() {
        CommandLine.usage(this, System.out);
    }

    public static void main(String[] args) {
        int code = new CommandLine(new SyncMgr()).execute(args);
        System.exit(code);
    }
}
