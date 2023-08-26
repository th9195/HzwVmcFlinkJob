package com.hzw.fdc.engine.controlwindow.exception;

public class DataFlowException extends Exception{

    public DataFlowException() {
    }

    public DataFlowException(String message) {
        super(message);
    }

    public DataFlowException(String message, Throwable cause) {
        super(message, cause);
    }
}
