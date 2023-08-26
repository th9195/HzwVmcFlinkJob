package com.hzw.fdc.engine.controlwindow.data.impl;

import com.hzw.fdc.engine.controlwindow.data.defs.IIndicatorData;
import com.hzw.fdc.engine.controlwindow.data.defs.ISensorData;

import java.io.Serializable;
import java.util.List;

public class IndicatorData implements IIndicatorData , Serializable {

    private static final long serialVersionUID = -6970301015436285743L;
    private List<ISensorData> data;
    private Long indicatorId;
    private Long windowId;
    private Long sensorAliasId;
    private String sensorAliasName;
    private Integer cycleUnitIndex = -1;

    private Long startTime;

    private Long stopTime;

    public String getSensorAliasName() {
        return sensorAliasName;
    }

    public void setSensorAliasName(String sensorAliasName) {
        this.sensorAliasName = sensorAliasName;
    }

    @Override
    public Long getSensorAliasId() {
        return sensorAliasId;
    }

    public void setSensorAliasId(Long sensorAliasId) {
        this.sensorAliasId = sensorAliasId;
    }

    @Override
    public List<ISensorData> getData() {
        return data;
    }

    public void setData(List<ISensorData> data) {
        this.data = data;
    }

    @Override
    public Long getIndicatorId() {
        return indicatorId;
    }

    public void setIndicatorId(Long indicatorId) {
        this.indicatorId = indicatorId;
    }

    @Override
    public Long getWindowId() {
        return windowId;
    }

    public void setWindowId(Long windowId) {
        this.windowId = windowId;
    }

    @Override
    public Integer getCycleUnitIndex() {
        return cycleUnitIndex;
    }

    @Override
    public WindowUnitPtr getParentWindowUnitPtr() {
        return null;
    }


    public void setCycleUnitIndex(Integer cycleUnitIndex) {
        this.cycleUnitIndex = cycleUnitIndex;
    }

    @Override
    public Long getStartTime() {
        return startTime;
    }

    @Override
    public void setStartTime(Long startTime) {
        this.startTime = startTime;
    }

    @Override
    public Long getStopTime() {
        return stopTime;
    }

    @Override
    public void setStopTime(Long stopTime) {
        this.stopTime = stopTime;
    }
}
