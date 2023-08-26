package com.hzw.fdc.engine.controlwindow.exception;

public class RealTimeWindowClipException extends Exception{

    public RealTimeWindowClipException() {
    }

    public RealTimeWindowClipException(String message) {
        super(message);
    }

    public RealTimeWindowClipException(String message, Throwable cause) {
        super(message, cause);
    }

    public RealTimeWindowClipException(Throwable cause) {
        super(cause);
    }

    public RealTimeWindowClipException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
