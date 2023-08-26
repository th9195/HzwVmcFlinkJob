package com.hzw.fdc.engine.calculatedindicator;

public interface ICalculatedIndicator {
    public double calculate() throws Throwable;
    public void setIndicatorValue(String indicatorId,String alias,  Double value) throws IndicatorIdNotMappedException;
}
