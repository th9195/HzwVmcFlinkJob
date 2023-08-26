package com.hzw.fdc.engine.controlwindow.data.impl;

import com.hzw.fdc.engine.controlwindow.data.defs.IDataPacket;
import com.hzw.fdc.engine.controlwindow.data.defs.ISensorData;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DataPacket implements IDataPacket, Serializable {

    private static final long serialVersionUID = -418383868220213935L;
    private long timestamp;
    private int stepId;
    private int rsn;
    private int index;
    private Map<String, ISensorData> sensorDataMap = new HashMap<>();

    public DataPacket(){}

    @Override
    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public Integer getStepId() {
        return stepId;
    }

    @Override
    public Integer getRSN() {
        return rsn;
    }

    @Override
    public void setRSN(Integer rsn) {
        this.rsn = rsn;
        for(ISensorData sensorData: sensorDataMap.values()){
            sensorData.setRsn(rsn);
        }
    }

    @Override
    public Set<ISensorData> getSensorDataList() {
        return new HashSet<>(sensorDataMap.values());
    }

    @Override
    public ISensorData getSensor(String sensorAlias) {
        return sensorDataMap.get(sensorAlias);
    }

    @Override
    public Set<String> getSensorAliasList() {
        return sensorDataMap.keySet();
    }

    @Override
    public void setStepId(int stepId) {
        this.stepId = stepId;
    }

    @Override
    public Map<String, ISensorData> getSensorDataMap() {
        return sensorDataMap;
    }

    @Override
    public void setSensorDataMap(Map<String, ISensorData> sensorDataMap) {
        this.sensorDataMap = sensorDataMap;
    }

    @Override
    public String toString() {
        return "DataPacket{" +
                "timestamp=" + timestamp +
                ", rsn="+rsn+
                ", stepId=" + stepId +
                ", sensorDataMap=" + sensorDataMap +
                ", index=" + index +
                '}';
    }

    public static DataPacket build(){
        return new DataPacket();
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
        for(ISensorData sensorData: sensorDataMap.values()){
            sensorData.setIndex(index);
        }
    }
}
