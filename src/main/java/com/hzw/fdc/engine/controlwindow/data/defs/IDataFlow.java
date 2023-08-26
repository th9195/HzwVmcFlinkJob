package com.hzw.fdc.engine.controlwindow.data.defs;

import com.hzw.fdc.engine.controlwindow.data.impl.DataFlow;
import com.hzw.fdc.engine.controlwindow.data.impl.Range;
import com.hzw.fdc.engine.controlwindow.exception.DataFlowException;

import java.io.Serializable;
import java.util.List;

public interface IDataFlow extends Serializable {

    public IDataFlow getSubDataList(int from, int end);

    public List<IDataPacket> getDataList();

    public List<ISensorData> getSensorDataList(String sensorId);

    public Range getStep(int from, int stepId);

    public List<DataFlow.StepPtr> getStepIds();

    public List<DataFlow.RSNPtr> getRSNs();

    public void sort();

    public void validate() throws DataFlowException;

    public List<IDataPacket> getDataPacketList();

    public void setDataPacketList(List<IDataPacket> dataPacketList);

    public boolean isSorted();

    public void setSorted(boolean sorted);

    public Long getStartTime() ;

    public void setStartTime(Long startTime);

    public Long getStopTime();

    public void setStopTime(Long stopTime);

    public void add(IDataPacket dataPacket);

    public List<String> getAllSensors();

}
