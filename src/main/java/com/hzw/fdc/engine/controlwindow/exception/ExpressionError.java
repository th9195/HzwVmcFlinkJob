package com.hzw.fdc.engine.controlwindow.exception;

public class ExpressionError extends Exception{
    public ExpressionError() {
        super();
    }

    public ExpressionError(String message) {
        super(message);
    }

    public ExpressionError(String message, Throwable cause) {
        super(message, cause);
    }
}
