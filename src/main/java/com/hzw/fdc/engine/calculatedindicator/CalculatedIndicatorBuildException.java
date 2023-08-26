package com.hzw.fdc.engine.calculatedindicator;

public class CalculatedIndicatorBuildException extends Exception{
    public CalculatedIndicatorBuildException() {
    }

    public CalculatedIndicatorBuildException(String message) {
        super(message);
    }

    public CalculatedIndicatorBuildException(String message, Throwable cause) {
        super(message, cause);
    }

    public CalculatedIndicatorBuildException(Throwable cause) {
        super(cause);
    }

    public CalculatedIndicatorBuildException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
