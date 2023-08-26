package com.hzw.fdc.engine.calculatedindicator;

public class IndicatorIdNotMappedException extends Exception{
    public IndicatorIdNotMappedException() {
    }

    public IndicatorIdNotMappedException(String message) {
        super(message);
    }

    public IndicatorIdNotMappedException(String message, Throwable cause) {
        super(message, cause);
    }
}
