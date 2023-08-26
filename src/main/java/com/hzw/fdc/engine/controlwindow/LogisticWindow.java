package com.hzw.fdc.engine.controlwindow;

import com.hzw.fdc.engine.controlwindow.data.defs.IDataFlow;
import com.hzw.fdc.engine.controlwindow.data.impl.Range;
import com.hzw.fdc.engine.controlwindow.exception.ControlWindowException;
import com.hzw.fdc.engine.controlwindow.exception.ControlWindowExpressionError;
import com.hzw.fdc.engine.controlwindow.expression.TimeOffset;
import com.hzw.fdc.engine.controlwindow.logistic.IExpressionScript;
import com.hzw.fdc.engine.controlwindow.logistic.SensorExpressionBuilder;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class LogisticWindow extends ContainerWindow implements Serializable {

    private static final long serialVersionUID = -1224291532738779879L;

    private  IExpressionScript startExpression;

    private  IExpressionScript endExpression;

    private String startExpStr = null;
    private String endExpStr = null;

    private String startOffset = "Tb0p";
    private String endOffset = "Te0p";

    public LogisticWindow() {

    }

    public LogisticWindow(Long id, String startExpStr,String endExpStr) throws ControlWindowExpressionError.LogisticControlWindowExpressionError {
        try {
            setWindowId(id);
            this.startExpStr = startExpStr;
            this.endExpStr = endExpStr;
            this.startExpression = new SensorExpressionBuilder(startExpStr).build();
            this.endExpression = new SensorExpressionBuilder(endExpStr).build();
        } catch (Throwable throwable) {
            throw  new ControlWindowExpressionError.LogisticControlWindowExpressionError(throwable.getMessage(),throwable);
        }
    }

    public void setExp(String start, String end) throws ControlWindowExpressionError.LogisticControlWindowExpressionError {
        this.startExpStr = start;
        this.endExpStr = end;
        try{
            if(ControlWindow.PRE_START.equals(start)){
                setFromBegin(true);
                start = "(_START) && offset(\"Tb0p\")";
            }else{
                if (start.contains(ControlWindow.PRE_START)) {
                    setFromBegin(true);
                } else {
                    setFromBegin(false);
                }
            }
            start = start.replace(ControlWindow.PRE_START, "true");
            this.startExpression = new SensorExpressionBuilder(start).build();
            if(ControlWindow.PRE_END.equals(end)){
                setToEnd(true);
                end = "(_END) && offset(\"Te0p\")";
            }else{
                if (end.contains(ControlWindow.PRE_END)) {
                    setToEnd(true);
                } else  {
                    setToEnd(false);
                }
            }
            end = end.replace(ControlWindow.PRE_END, "true");
            this.endExpression = new SensorExpressionBuilder(end).build();
        }catch (Throwable t){
            throw  new ControlWindowExpressionError.LogisticControlWindowExpressionError(t.getMessage(),t);
        }
    }

    public boolean isCanWindowStartPositionBeDetermined(Range startRange, IDataFlow dataFlow, boolean runEndFlag) throws ControlWindowExpressionError {
        // 判断区间是否是有效区间
        if(startRange.start==null || startRange.end==null){
            return false;
        }

        TimeOffset timeOffset = TimeOffset.parse(startOffset);
        if(timeOffset.direction=='b'){
            // 判断起始时间或起始位置是否在流的范围内
            int size = dataFlow.getDataList().size();
            long lastPointTime = dataFlow.getDataList().get(size - 1).getTimestamp();
            if ('s' == timeOffset.unit) {
                long windowStartTime = dataFlow.getDataList().get(startRange.start).getTimestamp() + timeOffset.value * 1000L;
                if (windowStartTime <= lastPointTime) {
                    return true;
                }
            } else if ('S' == timeOffset.unit) {
                long windowStartTime = dataFlow.getDataList().get(startRange.start).getTimestamp() + timeOffset.value;
                if (windowStartTime <= lastPointTime) {
                    return true;
                }
            } else if ('p' == timeOffset.unit) {
                int startPos = startRange.start + timeOffset.value;
                if (startPos < dataFlow.getDataList().size()) {
                    return true;
                }
            }
        } else {
            if (startRange.end < dataFlow.getDataList().size()) {
                return true;
            }
        }
        return runEndFlag;
    }

    public boolean isCanWindowEndPositionBeDetermined(Range endRange, IDataFlow dataFlow, boolean runEndFlag) throws ControlWindowExpressionError {
        // 判断区间是否是有效区间
        if(endRange.start==null || endRange.end==null){
            return false;
        }

        TimeOffset timeOffset = TimeOffset.parse(endOffset);
        if(timeOffset.direction=='b'){
            // 判断结束时间或结束位置是否在流的范围内
            int size = dataFlow.getDataList().size();
            long lastPointTime = dataFlow.getDataList().get(size - 1).getTimestamp();
            if ('s' == timeOffset.unit) {
                long windowEndTime = dataFlow.getDataList().get(endRange.start).getTimestamp() + timeOffset.value * 1000L;
                if (windowEndTime < lastPointTime) {
                    return true;
                }
            } else if ('S' == timeOffset.unit) {
                long windowEndTime = dataFlow.getDataList().get(endRange.start).getTimestamp() + timeOffset.value;
                if (windowEndTime < lastPointTime) {
                    return true;
                }
            } else if ('p' == timeOffset.unit) {
                int lastPos = endRange.start + timeOffset.value;
                if (lastPos < dataFlow.getDataList().size()) {
                    return true;
                }
            }
        } else {
            if (endRange.end < dataFlow.getDataList().size()) {
                return true;
            }
        }
        return runEndFlag;
    }
    @Override
    protected int[] postOffset(int startPosition, int endPosition) {
        try{
            try{
                if("Tb0s".equals(startOffset)||"Tb0S".equals(startOffset)){
                    startOffset = "Tb0p";
                }
                dataFlow.setByPoint(true);
            }catch (Throwable t){
            }

            int newStartPosition = -1;
            if (isCanWindowStartPositionBeDetermined(new Range(startPosition, endPosition), getDataFlow(), isCanWindowEndBeDetermined())) {
                TimeOffset timeOffset = TimeOffset.parse(startOffset).setEndPart(false);
                if (timeOffset.direction == 'b') {
//                if(startPosition==0){
////                    dataFlow.setSplitStartTime(dataFlow.getStartTime());
//                    dataFlow.setSplitStartTime(dataFlow.getDataList().get(startPosition).getTimestamp());
//                }else{
//                    dataFlow.setSplitStartTime(dataFlow.getDataList().get(startPosition).getTimestamp());
//                }
                    dataFlow.setSplitStartTime(dataFlow.getDataList().get(startPosition).getTimestamp());
                } else if (timeOffset.direction == 'e') {
                    if (endPosition == dataFlow.getDataList().size()) {
                        dataFlow.setSplitStartTime(dataFlow.getSplitStopTime());
                    } else {
                        dataFlow.setSplitStartTime(dataFlow.getDataList().get(endPosition).getTimestamp());
                    }
                }
                newStartPosition = timeOffset.eval(getDataFlow(), new Range(startPosition, endPosition));
            }

            int newEndPosition = -1;
            if (isCanWindowEndPositionBeDetermined(new Range(startPosition, endPosition), getDataFlow(), isCanWindowEndBeDetermined())) {
                TimeOffset timeOffset = TimeOffset.parse(endOffset).setEndPart(true);
                if (timeOffset.direction == 'e') {
                    if (endPosition == dataFlow.getDataList().size()) {
                        dataFlow.setSplitStopTime(dataFlow.getSplitStopTime());
                    } else {
                        dataFlow.setSplitStopTime(dataFlow.getDataList().get(endPosition).getTimestamp());
                    }
                } else if (timeOffset.direction == 'b') {
//                    if (endPosition == dataFlow.getDataList().size()) {
//                        dataFlow.setSplitStopTime(dataFlow.getStartTime());
//                    } else {
//                        dataFlow.setSplitStopTime(dataFlow.getDataList().get(startPosition).getTimestamp());
//                    }
                    dataFlow.setSplitStopTime(dataFlow.getDataList().get(startPosition).getTimestamp());
                }
                newEndPosition = timeOffset.eval(getDataFlow(), new Range(startPosition, endPosition));
            }

            return new int[]{newStartPosition,newEndPosition};
        }catch (Throwable t){
            t.printStackTrace();
        }
        return super.postOffset(startPosition, endPosition);
    }

    @Override
    public int getStartPosition() throws ControlWindowException {
        if(startExpression==null){
            try {
                setExp(this.startExpStr,this.endExpStr);
            } catch (ControlWindowExpressionError.LogisticControlWindowExpressionError logisticControlWindowExpressionError) {
                throw new ControlWindowException("Logistic Window Error: No Start Expression Found. Possibly caused by de-serialization");
            }
        }

        ((AbstractExpressionScript)startExpression).setEndPart(false);
        ((AbstractExpressionScript)startExpression).setControlWindow(this);
        if (isFromBegin()) {
            if (getAttachedDataFlow().getDataList().size() == 0) {
                throw new ControlWindowException("[Logistic] No END matched: dataflow is empty");
            }
            try {
                AbstractExpressionScript.DataPackIndex dataPackIndex = new AbstractExpressionScript.DataPackIndex(0);
                startExpression.eval(getAttachedDataFlow().getDataList().get(0), dataPackIndex, this, new Range(0, 1));
            } catch (Throwable throwable) {
                throw new ControlWindowException("Logistic Calculation Error: "+throwable.getMessage());
            }
            return 0;
        }

        for(int i=0;i<getAttachedDataFlow().getDataList().size();i++){
            try {

                AbstractExpressionScript.DataPackIndex dataPackIndex = new AbstractExpressionScript.DataPackIndex(i);
                if(startExpression.eval(getAttachedDataFlow().getDataList().get(i), dataPackIndex,this, new Range(i,getAttachedDataFlow().getDataList().size()))){
                    return dataPackIndex.getIndex();
                }
            } catch (Throwable throwable) {
                throw new ControlWindowException("Logistic Calculation Error: "+throwable.getMessage());
            }
        }
        throw new ControlWindowException("No START matched");
    }

    @Override
    public int getEndPosition() throws ControlWindowException {
        if(endExpression==null){
            try {
                setExp(this.startExpStr,this.endExpStr);
            } catch (ControlWindowExpressionError.LogisticControlWindowExpressionError logisticControlWindowExpressionError) {
                throw new ControlWindowException("Logistic Window Error: No End Expression Found. Possibly caused by de-serialization");
            }
        }

        ((AbstractExpressionScript)endExpression).setEndPart(true);
        ((AbstractExpressionScript)endExpression).setControlWindow(this);
        if (isToEnd()) {
            if (getAttachedDataFlow().getDataList().size() == 0) {
                throw new ControlWindowException("[Logistic] No END matched: dataflow is empty");
            }
            try {
                AbstractExpressionScript.DataPackIndex dataPackIndex = new AbstractExpressionScript.DataPackIndex(0);
                endExpression.eval(getAttachedDataFlow().getDataList().get(0), dataPackIndex, this, new Range(0, 1));
            } catch (Throwable throwable) {
                throw new ControlWindowException("Logistic Calculation Error: "+throwable.getMessage());
            }
            return getAttachedDataFlow().getDataList().size();
        }

        int start = getStartPosition()+1;
        for(int i=start;i<getAttachedDataFlow().getDataList().size();i++){
            try {
                AbstractExpressionScript.DataPackIndex dataPackIndex = new AbstractExpressionScript.DataPackIndex(i);
                if(endExpression.eval(getAttachedDataFlow().getDataList().get(i), dataPackIndex,this,new Range(0,i))){
                    return dataPackIndex.getIndex()+1;
                }
            } catch (Throwable throwable) {
                throw new ControlWindowException("Logistic Calculation Error: "+throwable.getMessage());
            }
        }
        throw new ControlWindowException("No END matched");
    }


    public List<String> getRequiredSensors(){
        List<String> all = new ArrayList<>();
        try{
            AbstractExpressionScript start = (AbstractExpressionScript) startExpression;
            all.addAll(start.getVariables());

        }catch (Throwable t){
            t.printStackTrace();
        }
        try{

            AbstractExpressionScript end = (AbstractExpressionScript) endExpression;
            all.addAll(end.getVariables());

        }catch (Throwable t){
            t.printStackTrace();
        }
        return all;
    }


    public String getStartExpStr() {
        return startExpStr;
    }

    public void setStartExpStr(String startExpStr) {
        this.startExpStr = startExpStr;
        try {
            this.startExpression = new SensorExpressionBuilder(startExpStr).build();
        } catch (Throwable throwable) {
        }
    }

    public String getEndExpStr() {
        return endExpStr;
    }

    public void setEndExpStr(String endExpStr) {
        this.endExpStr = endExpStr;
        try {
            this.endExpression = new SensorExpressionBuilder(endExpStr).build();
        } catch (Throwable throwable) {
        }
    }

    public String getStartOffset() {
        return startOffset;
    }

    public void setStartOffset(String startOffset) {
        this.startOffset = startOffset;
    }

    public String getEndOffset() {
        return endOffset;
    }

    public void setEndOffset(String endOffset) {
        this.endOffset = endOffset;
    }
}
