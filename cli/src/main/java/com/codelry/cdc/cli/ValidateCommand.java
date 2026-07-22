package com.codelry.cdc.cli;

import java.nio.file.Path;
import java.util.concurrent.Callable;

import com.codelry.cdc.config.ConfigLoader;
import com.codelry.cdc.config.ConfigValidationException;
import com.codelry.cdc.config.secret.SecretResolutionException;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "validate", description = "Validate connection.yaml and mapping.yaml (JSON Schema + secret refs).")
public class ValidateCommand implements Callable<Integer> {

    @Option(names = {"-c", "--connection"}, required = true, description = "Path to connection.yaml")
    Path connection;

    @Option(names = {"-m", "--mapping"}, required = true, description = "Path to mapping.yaml")
    Path mapping;

    @Option(names = {"--syntax-only"}, description = "Validate schema only; skip ${env:}/${vault:} resolution")
    boolean syntaxOnly;

    @Override
    public Integer call() {
        try {
            ConfigLoader loader = new ConfigLoader();
            if (syntaxOnly) {
                loader.validateConnectionSyntax(connection);
                loader.validateMappingSyntax(mapping);
            } else {
                loader.loadBundle(connection, mapping);
            }
            System.out.println("OK: configuration is valid");
            return 0;
        } catch (ConfigValidationException | SecretResolutionException e) {
            System.err.println("INVALID: " + e.getMessage());
            return 1;
        } catch (Exception e) {
            System.err.println("ERROR: " + e.getMessage());
            return 2;
        }
    }
}
