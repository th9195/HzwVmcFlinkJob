package com.hzw.fdc.engine.controlwindow.data.defs;

import java.util.Map;

public interface ISensorData {
    public String getSensorAlias();
    public String getSensorName();

    void setSensorAlias(String sensorAlias);

    public double getValue();

    void setValue(double value);

    public int getStepId();

    void setStepId(int stepId);

    void setExtras(Map<String, Object> extras);

    public long getTimestamp();

    void setTimestamp(long timestamp);

    public String getUnit();

    void setSensorName(String sensorName);

    public Map<String,Object> getExtras();

    void setUnit(String unit);

    public int getIndex();
    public void setIndex(int index);

    public int getRsn();

    public void setRsn(int rsn);
}
