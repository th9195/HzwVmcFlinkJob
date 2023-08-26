package com.hzw.fdc.engine.controlwindow.data.defs;

import java.util.Map;
import java.util.Set;

public interface IDataPacket {

    public long getTimestamp();
    public void setTimestamp(long timestamp);
    public Integer getStepId(); // which can be extracted from a special sensor
    public Integer getRSN();
    public void setRSN(Integer rsn);
    Set<ISensorData> getSensorDataList();
    public ISensorData getSensor(String sensorAlias);
    public Set<String> getSensorAliasList();
    void setStepId(int stepId);
    Map<String, ISensorData> getSensorDataMap();
    public int getIndex();
    public void setIndex(int index);
    void setSensorDataMap(Map<String, ISensorData> sensorDataMap);
}
