package com.hzw.fdc.engine.controlwindow.data.impl;

import com.hzw.fdc.engine.controlwindow.data.defs.IDataFlow;
import com.hzw.fdc.engine.controlwindow.data.defs.IDataPacket;
import com.hzw.fdc.engine.controlwindow.data.defs.ISensorData;
import com.hzw.fdc.engine.controlwindow.exception.DataFlowException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

public class DataFlow implements IDataFlow, Serializable {

    private static final long serialVersionUID = -1837036753692651510L;

    static Logger log = LoggerFactory.getLogger(DataFlow.class);

    private List<IDataPacket> dataPacketList = new ArrayList<>();

    private boolean isSorted = false;

    private Long startTime;

    private Long stopTime;

    private Long splitStartTime;

    private Long splitStopTime;

    private boolean isAutoStartBound = true;

    private boolean isAutoEndBound = true;

    private boolean isByPoint = false;

    public boolean isAutoStartBound() {
        return isAutoStartBound;
    }

    public void setAutoStartBound(boolean autoStartBound) {
        isAutoStartBound = autoStartBound;
    }

    public boolean isAutoEndBound() {
        return isAutoEndBound;
    }

    public void setAutoEndBound(boolean autoEndBound) {
        isAutoEndBound = autoEndBound;
    }

    public Long getSplitStartTime() {

        return splitStartTime;
    }

    public void setSplitStartTime(Long splitStartTime) {
        this.splitStartTime = splitStartTime;
    }

    public Long getSplitStopTime() {

        return splitStopTime;
    }

    public void setSplitStopTime(Long splitStopTime) {
        this.splitStopTime = splitStopTime;
    }

    public DataFlow() { }

    /**
     *
     * @param from 闭区间
     * @param end (exclusive) 开区间
     * @return
     */
    @Override
    public IDataFlow getSubDataList(int from, int end) {
        DataFlow dataFlow = new DataFlow();
        dataFlow.setSorted(isSorted);
        if(isAutoStartBound) {
            if (from == 0) {
                if(isByPoint){
                    dataFlow.setStartTime(splitStartTime);
                }else{
                    dataFlow.setStartTime(startTime);
                }

            } else {
                dataFlow.setStartTime(dataPacketList.get(from).getTimestamp());
            }
        }else{
            if (startTime == null) {
                long firstPointTime = getDataList().get(0).getTimestamp();
                dataFlow.setStartTime(splitStartTime < firstPointTime ? firstPointTime : splitStartTime);
            } else {
                dataFlow.setStartTime(splitStartTime < startTime ? startTime : splitStartTime);
            }
        }

        if(isAutoEndBound){
            int size = dataPacketList.size();
            if(end==size){
                dataFlow.setStopTime(stopTime);
            }else{
                dataFlow.setStopTime(dataPacketList.get(end).getTimestamp());
            }
        }else{
            if (stopTime == null) {
                int size = getDataList().size();
                long lastPointTime = getDataList().get(size - 1).getTimestamp();
                dataFlow.setStopTime(splitStopTime > lastPointTime ? lastPointTime : splitStopTime);
            } else {
                dataFlow.setStopTime(splitStopTime > stopTime ? stopTime : splitStopTime);
            }
        }
        dataFlow.setDataPacketList(new ArrayList<>(dataPacketList.subList(from,end)));
        return dataFlow;
    }

    @Override
    public List<IDataPacket> getDataList() {
        return dataPacketList;
    }

    @Override
    public List<ISensorData> getSensorDataList(String sensorAlias) {
        List<ISensorData> list = new ArrayList<>();
        ISensorData ptr = null;
        for(IDataPacket dataPacket: dataPacketList){
            ptr = dataPacket.getSensor(sensorAlias);
            if(ptr!=null){
                list.add(ptr);
            }
        }
        return list;
    }

    public void setDataPacketList(List<IDataPacket> dataPacketList) {
        this.dataPacketList = dataPacketList;
    }

    public Range getStep(int from, int stepId){
        Range range = new Range();
        boolean found  = false;
        for(int i=from;i<dataPacketList.size();i++){
            if(!found){
                if(dataPacketList.get(i).getStepId()==stepId){
                    found=true;
                    range.setStart(i);
                }
            }else{
                if(dataPacketList.get(i).getStepId()!=stepId){
                    range.setEnd(i-1);
                    break;
                }
            }
        }
        return range;
    }

    public List<StepPtr> getStepIds(){
        List<StepPtr> list = new ArrayList<>();
        boolean found  = false;

        StepPtr stepPtr = null;
        for(int i=0;i<dataPacketList.size();){
            if(stepPtr==null){
                stepPtr = new StepPtr();
                stepPtr.stepId = dataPacketList.get(i).getStepId();

            }

            if(!found){
                Object o1 = dataPacketList.get(i).getStepId();
                Object o2 = stepPtr.stepId;

                if(dataPacketList.get(i).getStepId().equals(stepPtr.stepId)){
                    found=true;
                    list.add(stepPtr);
                    stepPtr.range.setStart(i);
                }
                i++;
            }else{
                if(!dataPacketList.get(i).getStepId().equals(stepPtr.stepId)){
                    //如果当前数据点的stepId和当前step游标的stepId不同，说明发生了变化
                    found = false;
                    stepPtr.range.setEnd(i);
                    stepPtr = null;
                }else{
                    //说明没有发生变化
                    i++;
                }
            }
        }

        if((stepPtr!=null)&&(stepPtr.range!=null)&&(stepPtr.range.end==null)){
            stepPtr.range.end = dataPacketList.size();
        }
        return list;
    }

    public List<RSNPtr> getRSNs(){
        List<StepPtr> stepIds = getStepIds();
        List<RSNPtr> rsnList = new ArrayList<>();
        int max = Integer.MIN_VALUE;
        RSNPtr rsnPtr = new RSNPtr();
        StepPtr step = null;
        int rsnNumber = 1;
        rsnPtr.rsnNumber = rsnNumber;
        for(int i=0;i<stepIds.size();){
            step = stepIds.get(i);
            if(step.stepId>max){
                max = step.stepId;
                rsnPtr.steps.add(step);

                i++;
            }else{
                max = Integer.MIN_VALUE;
                rsnList.add(rsnPtr);
                rsnPtr = new RSNPtr();
                rsnNumber++;
                rsnPtr.rsnNumber = rsnNumber;
            }
        }
        if(!rsnList.contains(rsnPtr)){
            rsnList.add(rsnPtr);
        }

        return rsnList;
    }

    public void sort(){
        if(!isSorted){
            dataPacketList.sort((o1, o2) -> (int) (o1.getTimestamp()-o2.getTimestamp()));
            updateIndex();
            isSorted = true;
        }
        updateRSNInformation();

    }

    private void updateIndex() {
        for(int i=0;i<dataPacketList.size();i++){
            dataPacketList.get(i).setIndex(i);
        }
    }

    private void updateRSNInformation() {
        List<RSNPtr> list = getRSNs();
        int dataPacketListLen = dataPacketList.size();
        for(RSNPtr rsnPtr: list){
            for(StepPtr stepPtr: rsnPtr.steps){
                for(int i=stepPtr.range.start;i<stepPtr.range.end;i++){
                    if(i>=0 && (i<dataPacketListLen)){
                        dataPacketList.get(i).setRSN(rsnPtr.rsnNumber);
                    }
                }
            }
        }
    }

    @Override
    public void validate() throws DataFlowException {
        for(IDataPacket dp: dataPacketList){
            if((dp.getStepId()==-99)||(dp.getStepId()==null)){
                throw new DataFlowException("出现了stepId=-99或stepId=null的数据点。");
            }
        }
    }

    public static class StepPtr implements Serializable{
        private static final long serialVersionUID = 708502178094732881L;
        public int stepId;
        public Range range = new Range();

        @Override
        public String toString() {
            return "Step{"+stepId+"}->"+range;
        }
    }

    public static class RSNPtr implements Serializable {

        private static final long serialVersionUID = 8834184711908906041L;
        public int rsnNumber;
        public List<StepPtr> steps = new ArrayList<>();

        @Override
        public String toString() {
            return "RSN{"+rsnNumber+"}->"+steps;
        }

        public Range getRange(){
            Range range = new Range();
            range.setStart(steps.get(0).range.getStart());
            range.setEnd(steps.get(steps.size()-1).range.getEnd());
            return range;
        }
    }

    /**
     * 返回一组纯数据对象，方便用于数据json化
     */
    public List<Map> toListData(){
        List<Map> list = new ArrayList<>();
        for(IDataPacket dataPacket: dataPacketList){
            Map map = new HashMap();
            map.put("timestamp",dataPacket.getTimestamp());
            map.put("stepId",dataPacket.getStepId());
            List<Map> sensors = new ArrayList<>();
            Set<ISensorData> set = dataPacket.getSensorDataList();
            for(ISensorData sd: set){
                Map sensor = new HashMap();
                sensor.put("timestamp",sd.getTimestamp());
                sensor.put("stepId",sd.getStepId());
                sensor.put("value",sd.getValue());
                sensor.put("sensorAlias",sd.getSensorAlias());
                sensor.put("sensorName",sd.getSensorName());
                sensors.add(sensor);
            }
            map.put("sensorDatas",sensors);
            list.add(map);
        }

        return list;
    }

    public static DataFlow build(){
        return new DataFlow();
    }

    public static DataFlow build(IDataFlow dataFlow){
        DataFlow nf = new DataFlow();
        List<IDataPacket> list = dataFlow.getDataList();
        for(IDataPacket dataPacket: list){
            nf.getDataList().add(dataPacket);
        }
        nf.setSorted(dataFlow.isSorted());
        return nf;
    }


    public void add(IDataPacket dataPacket){
        if(!existSameTimeStampData(dataPacket)){
            getDataList().add(dataPacket);
        }
    }

    private boolean existSameTimeStampData(IDataPacket dataPacket){
        List<IDataPacket> list = getDataList();
        for(IDataPacket dp: list){
            if(dp.getTimestamp() == dataPacket.getTimestamp()){
                return true;
            }
        }
        return false;
    }

    public List<IDataPacket> getDataPacketList() {
        return dataPacketList;
    }

    public void validateDataIntegrity(){

    }

    public boolean isSorted() {
        return isSorted;
    }

    public void setSorted(boolean sorted) {
        isSorted = sorted;
    }

    public Long getStartTime() {
        return startTime;
    }

    public void setStartTime(Long startTime) {
        this.startTime = startTime;
        setSplitStartTime(startTime);
    }

    public Long getStopTime() {
        return stopTime;
    }

    public void setStopTime(Long stopTime) {
        this.stopTime = stopTime;
        setSplitStopTime(stopTime);
    }

    public List<String> getAllSensors(){
        Set<String> tmp = new HashSet<>();
        for(IDataPacket dataPacket: dataPacketList){
            tmp.addAll(dataPacket.getSensorAliasList());
        }
        List<String> sensors = new ArrayList<>();
        sensors.addAll(tmp);
        Collections.sort(sensors);
        return sensors;
    }

    public boolean isByPoint() {
        return isByPoint;
    }

    public void setByPoint(boolean byPoint) {
        isByPoint = byPoint;
    }

    public DataFlow cloneSelf(Set<String> sensorAlias){
        DataFlow dataFlow = new DataFlow();
        for(IDataPacket dataPacket: dataPacketList){
            DataPacket dp = new DataPacket();
            for(String sensorKey: dataPacket.getSensorDataMap().keySet()){
                if(!sensorAlias.contains(sensorKey)){
                    continue;
                }
                ISensorData sensorData = dataPacket.getSensorDataMap().get(sensorKey);
                SensorData newSensorData = SensorData.build();
                newSensorData.setStepId(sensorData.getStepId());
                newSensorData.setSensorAlias(sensorData.getSensorAlias());
                newSensorData.setValue(sensorData.getValue());
                newSensorData.setTimestamp(sensorData.getTimestamp());
                newSensorData.setIndex(sensorData.getIndex());
                newSensorData.setRsn(sensorData.getRsn());
                newSensorData.setUnit(sensorData.getUnit());
                newSensorData.setSensorName(sensorData.getSensorName());
                newSensorData.setExtras(sensorData.getExtras());
                dp.getSensorDataMap().put(sensorKey, newSensorData);
            }
            dp.setIndex(dataPacket.getIndex());
            dp.setTimestamp(dataPacket.getTimestamp());
            dp.setStepId(dataPacket.getStepId());
            dp.setRSN(dataPacket.getRSN());
            // 如果一个数据包里面一个sensor都没有，则不要这个数据点。根据需求：FAB6-318
            if(!dp.getSensorDataMap().isEmpty()){
                dataFlow.getDataList().add(dp);
            }
        }
        dataFlow.setStartTime(getStartTime());
        dataFlow.setStopTime(getStopTime());
        dataFlow.setAutoEndBound(isAutoEndBound());
        dataFlow.setAutoStartBound(isAutoStartBound());
        dataFlow.setByPoint(isByPoint());
        dataFlow.setSorted(isSorted());
        dataFlow.setSplitStartTime(getSplitStartTime());
        dataFlow.setSplitStopTime(getSplitStopTime());
        return dataFlow;
    }


}
