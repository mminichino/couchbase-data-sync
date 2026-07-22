package com.codelry.cdc.config;

public class ConfigValidationException extends RuntimeException {

    public ConfigValidationException(String message) {
        super(message);
    }

    public ConfigValidationException(String message, Throwable cause) {
        super(message, cause);
    }
}
