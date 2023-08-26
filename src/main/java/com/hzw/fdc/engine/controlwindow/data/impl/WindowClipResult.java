package com.hzw.fdc.engine.controlwindow.data.impl;

import com.hzw.fdc.engine.controlwindow.data.defs.IDataPacket;
import com.hzw.fdc.engine.controlwindow.data.defs.ISensorData;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;


public class WindowClipResult implements Serializable {

    private static final long serialVersionUID = -2709690668254327693L;

    private Long windowId;
    private Long sensorAliasId;
    private List<ISensorData> data = null;
    private List<Long> indicatorIds = new ArrayList<>();
    private Integer cycleUnitIndex = -1;
    private Integer cycleUnitCount = 1;
    private Long startTime;
    private Long stopTime;
    private Double startTimeValue = null;
    private Double stopTimeValue = null;

    public Long getWindowId() {
        return windowId;
    }

    public void setWindowId(Long windowId) {
        this.windowId = windowId;
    }

    public Long getSensorAliasId() {
        return sensorAliasId;
    }

    public void setSensorAliasId(Long sensorAliasId) {
        this.sensorAliasId = sensorAliasId;
    }

    public List<ISensorData> getData() {
        return data;
    }

    public void setData(List<ISensorData> data) {
        this.data = data;
    }

    public List<Long> getIndicatorIds() {
        return indicatorIds;
    }

    public void setIndicatorIds(List<Long> indicatorIds) {
        this.indicatorIds = indicatorIds;
    }

    public Integer getCycleUnitIndex() {
        return cycleUnitIndex;
    }

    public void setCycleUnitIndex(Integer cycleUnitIndex) {
        this.cycleUnitIndex = cycleUnitIndex;
    }

    public Integer getCycleUnitCount() {
        return cycleUnitCount;
    }

    public void setCycleUnitCount(Integer cycleUnitCount) {
        this.cycleUnitCount = cycleUnitCount;
    }

    public String getWindowClipResultKey(){
        if(indicatorIds.isEmpty()){
            return StringUtils.join(windowId,"_",sensorAliasId);
        }else{
            return StringUtils.join(windowId,"_",indicatorIds.get(0),"_",sensorAliasId);
        }
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WindowClipResult that = (WindowClipResult) o;
        return Objects.equals(windowId, that.windowId) &&
                Objects.equals(sensorAliasId, that.sensorAliasId) &&
                Objects.equals(cycleUnitIndex, that.cycleUnitIndex);
    }

    @Override
    public String toString() {
        return "WindowClipResult{" +
                "windowId=" + windowId +
                ", sensorAliasId="+sensorAliasId+
                ", data=" + data +
                ", indicatorIds=" + indicatorIds +
                ", startTime=" + startTime +
                ", stopTime=" + stopTime +
                '}';
    }

    @Override
    public int hashCode() {
        return Objects.hash(windowId, sensorAliasId, cycleUnitIndex);
    }


    public Long getStartTime(){
        return startTime;
    }

    public void setStartTime(Long startTime) {
        this.startTime = startTime;
    }

    public Long getStopTime() {
        return stopTime;
    }

    public void setStopTime(Long stopTime) {
        this.stopTime = stopTime;
    }

    public Long getEndTime(){
        return stopTime;
    }

    public Double getStartTimeValue() {
        return startTimeValue;
    }

    public void setStartTimeValue(Double startTimeValue) {
        this.startTimeValue = startTimeValue;
    }

    public Double getStopTimeValue() {
        return stopTimeValue;
    }

    public void setStopTimeValue(Double stopTimeValue) {
        this.stopTimeValue = stopTimeValue;
    }

    public void proceedBoundValue(DataFlow dataFlow){
        if(dataFlow.getDataPacketList().size()==0){
            return;
        }
        if(this.data==null) return;
        if(this.data.size()==0) return;
        String sa = this.data.get(0).getSensorAlias();
        if(this.startTime==null){
            this.startTimeValue = null;
        }
        if(this.stopTime == null){
            this.stopTimeValue = null;
        }

        //find start value
        List<IDataPacket> list = dataFlow.getDataPacketList();
        IDataPacket dp = null;
        ISensorData sd = null;

        for(int i=0;i<list.size();i++){
            dp = list.get(i);
            if((Long)dp.getTimestamp() == this.startTime){
                sd = dp.getSensor(sa);
                this.startTimeValue = sd.getValue();
            }
//            System.out.println(this);

            if((Long)dp.getTimestamp() == this.stopTime){
                sd = dp.getSensor(sa);
                this.stopTimeValue = sd.getValue();
            }
        }

    }
}
