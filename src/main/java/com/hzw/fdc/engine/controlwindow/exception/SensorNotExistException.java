package com.hzw.fdc.engine.controlwindow.exception;

public class SensorNotExistException extends Exception{
    public SensorNotExistException() {
    }

    public SensorNotExistException(String message) {
        super(message);
    }

    public SensorNotExistException(String message, Throwable cause) {
        super(message, cause);
    }
}
