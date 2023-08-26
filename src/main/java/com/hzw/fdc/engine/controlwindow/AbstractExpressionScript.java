package com.hzw.fdc.engine.controlwindow;

import com.hzw.fdc.engine.controlwindow.data.defs.IDataPacket;
import com.hzw.fdc.engine.controlwindow.data.defs.ISensorData;
import com.hzw.fdc.engine.controlwindow.data.impl.DataFlow;
import com.hzw.fdc.engine.controlwindow.data.impl.Range;
import com.hzw.fdc.engine.controlwindow.exception.SensorNotExistException;
import com.hzw.fdc.engine.controlwindow.expression.TimeOffset;
import com.hzw.fdc.engine.controlwindow.logistic.IExpressionScript;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class AbstractExpressionScript implements IExpressionScript, Serializable {

    static Logger logger = LoggerFactory.getLogger(AbstractExpressionScript.class);

    private static final long serialVersionUID = 28619919792096691L;
    IDataPacket dataPacket;

    List<String> variables = new ArrayList<>();

    Map<String,Object> sensorMap = new HashMap<>();

    Class _cls = null;

    private DataPackIndex dataPackIndex;

    private ControlWindow controlWindow = null;

    private boolean isEndPart = false;

    private Range range = null;


    public List<String> getVariables() {
        return variables;
    }

    public void setVariables(List<String> variables) {
        this.variables = variables;
    }

    private void setSensorValue(String sensorAlias) throws Exception {
        if(dataPacket.getSensor(sensorAlias)==null){
            throw new SensorNotExistException(sensorAlias+" not exist");
        }
        ISensorData sensorData = dataPacket.getSensor(sensorAlias);
        String sensorAliasVariable = sensorData.getSensorAlias();
        sensorMap.put(sensorAlias,sensorData.getValue());
        _setField(sensorAliasVariable,sensorData.getValue());

    }

    private void _setField(String fieldName,Object value) throws SensorNotExistException {
        try {
            if(_cls==null){
                _cls = getClass();
            }
            Field field = _cls.getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(this,value);
        } catch (NoSuchFieldException|IllegalAccessException e) {
            throw new SensorNotExistException(fieldName+" not exist.",e);
        }
    }

    @Override
    public boolean eval(IDataPacket dataPacket, DataPackIndex index, ControlWindow controlWindow, Range range) throws Throwable {
        this.dataPackIndex = index;
        this.dataPacket = dataPacket;
        this.range = range;
        this.controlWindow = controlWindow;
        for(String sa: variables){
            try {
                setSensorValue(sa);
            }catch (SensorNotExistException sensorNotExistException){
//                logger.warn("wow=>[LogisticWindow][SensorNotExist]: {}",sensorNotExistException.getMessage());
                return false;
            } catch (Throwable e) {
                throw e;
            }
        }
        return expression();
    }

    public abstract boolean expression() throws Throwable;

    public Integer RSN(){
        return dataPacket.getRSN();
    }

    public boolean offset(String exp) throws Throwable{
        if(isEndPart()){
            ((LogisticWindow)controlWindow).setEndOffset(exp);
        }else{
            ((LogisticWindow)controlWindow).setStartOffset(exp);
        }
//        try{
//
//            int oldIndex = dataPackIndex.index;
//            TimeOffset offset = TimeOffset.parse(exp);
//            offset.setEndPart(isEndPart());
//            offset.setSplitTime((DataFlow) controlWindow.getAttachedDataFlow(),controlWindow.getAttachedDataFlow().getDataList().get(oldIndex).getTimestamp());
//            int index = offset.eval(controlWindow.getAttachedDataFlow(),range);
//            dataPackIndex.setIndex(index);
//
//        }catch (Throwable t){
//            throw t;
//        }
        return true;
    }

    public static class DataPackIndex {


        private Integer index;

        public DataPackIndex() {
        }

        public DataPackIndex(Integer index) {
            this.index = index;
        }

        public Integer getIndex() {
            return index;
        }

        public void setIndex(Integer index) {
            this.index = index;
        }
    }


    public Double $(String sensor) throws SensorNotExistException{
        if(sensorMap.containsKey(sensor)){
            return (Double) sensorMap.get(sensor);
        }else{
            throw new SensorNotExistException(sensor);
        }

    }

    public String $$(String sensor) throws SensorNotExistException {
        if(sensorMap.containsKey(sensor)){
            return  sensorMap.get(sensor)+"";
        }else{
            throw new SensorNotExistException(sensor);
        }

    }

    public IDataPacket getDataPacket() {
        return dataPacket;
    }

    public void setDataPacket(IDataPacket dataPacket) {
        this.dataPacket = dataPacket;
    }

    public Map<String, Object> getSensorMap() {
        return sensorMap;
    }

    public void setSensorMap(Map<String, Object> sensorMap) {
        this.sensorMap = sensorMap;
    }

    public Class get_cls() {
        return _cls;
    }

    public void set_cls(Class _cls) {
        this._cls = _cls;
    }

    public DataPackIndex getDataPackIndex() {
        return dataPackIndex;
    }

    public void setDataPackIndex(DataPackIndex dataPackIndex) {
        this.dataPackIndex = dataPackIndex;
    }

    public ControlWindow getControlWindow() {
        return controlWindow;
    }

    public void setControlWindow(ControlWindow controlWindow) {
        this.controlWindow = controlWindow;
    }

    public Range getRange() {
        return range;
    }

    public void setRange(Range range) {
        this.range = range;
    }

    public boolean isEndPart() {
        return isEndPart;
    }

    public AbstractExpressionScript setEndPart(boolean endPart) {
        isEndPart = endPart;
        return this;
    }

}
