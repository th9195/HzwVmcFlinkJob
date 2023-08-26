package com.hzw.fdc.engine.calculatedindicator;

public class IndicatorTimeLimitExceedException extends RuntimeException{
    public IndicatorTimeLimitExceedException() {
    }

    public IndicatorTimeLimitExceedException(long realTime,long time) {

        super(String.format("Calculated Indicator time limit exceeds: %d > %d",realTime,time));
    }

    public IndicatorTimeLimitExceedException(String message, Throwable cause) {
        super(message, cause);
    }
}
