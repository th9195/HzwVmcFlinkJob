package com.hzw.fdc.engine.controlwindow.data.impl;

import com.hzw.fdc.engine.controlwindow.data.defs.ISensorData;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class SensorData implements ISensorData, Serializable {
    private static final long serialVersionUID = -7562127990792896733L;
    private String sensorAlias;
    private String sensorName="";
    private double value;
    private int stepId;
    private long timestamp;
    private int rsn;
    private int index;

    private String unit;

    private Map<String,Object> extras = new HashMap<>();

    private SensorData(){}

    @Override
    public String getSensorAlias() {
        return sensorAlias;
    }

    @Override
    public String getSensorName() {
        return null;
    }

    @Override
    public void setSensorAlias(String sensorAlias) {
        this.sensorAlias = sensorAlias;
    }

    @Override
    public double getValue() {
        return value;
    }

    @Override
    public void setValue(double value) {
        this.value = value;
    }

    @Override
    public int getStepId() {
        return stepId;
    }

    @Override
    public void setSensorName(String sensorName) {
        this.sensorName = sensorName;
    }

    @Override
    public Map<String, Object> getExtras() {
        return null;
    }

    @Override
    public void setStepId(int stepId) {
        this.stepId = stepId;
    }

    @Override
    public void setExtras(Map<String, Object> extras) {
        this.extras = extras;
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String getUnit() {
        return unit;
    }

    @Override
    public void setUnit(String unit) {
        this.unit = unit;
    }

    @Override
    public String toString() {
        return "SensorData{" +
                "sensorAlias='" + sensorAlias + '\'' +
                "sensorName='" + sensorName + '\'' +
                ", value=" + value +
                ", stepId=" + stepId +
                ", index=" + index +
                ", rsn=" + rsn +
                '}';
    }

    public static SensorData build(){
        return new SensorData();
    }

    @Override
    public int getIndex() {
        return index;
    }

    @Override
    public void setIndex(int index) {
        this.index = index;
    }

    public int getRsn() {
        return rsn;
    }

    public void setRsn(int rsn) {
        this.rsn = rsn;
    }
}
