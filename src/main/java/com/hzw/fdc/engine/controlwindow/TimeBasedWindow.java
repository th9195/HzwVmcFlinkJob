package com.hzw.fdc.engine.controlwindow;

import com.hzw.fdc.engine.controlwindow.data.defs.IDataFlow;
import com.hzw.fdc.engine.controlwindow.data.impl.Range;
import com.hzw.fdc.engine.controlwindow.exception.ControlWindowException;
import com.hzw.fdc.engine.controlwindow.expression.TimeOffset;

import java.io.Serializable;
import java.text.NumberFormat;

public  class TimeBasedWindow extends ContainerWindow implements Serializable {

    private static final long serialVersionUID = 4560987334919598270L;
    /**
     *  0: by time in second
     *  1: by point in count
     */
    protected char unit_start = 's';
    protected char unit_end = 'p';

    /**
     * 0: b, from begin
     * 1: e, from end
     */
    protected char direction_start = 'b';
    protected char direction_end = 'e';

    /**
     *  if unit == 0, value indicate time in seconds
     *  if unit == 1, value indicator points in direction
     */
    protected int value_start = 0;
    protected int value_end = 0;

    public TimeBasedWindow() {
    }

    public TimeBasedWindow(Long id, char unit_start,char direction_start, int value_start,  char unit_end, char direction_end,  int value_end) {
        setWindowId(id);
        this.unit_start = unit_start;
        this.unit_end = unit_end;
        this.direction_start = direction_start;
        this.direction_end = direction_end;
        this.value_start = value_start;
        this.value_end = value_end;
    }

    protected Range prepareStart(){
        return new Range(0,getAttachedDataFlow().getDataList().size());
    }

    protected Range prepareEnd(){
        return new Range(0,getAttachedDataFlow().getDataList().size());
    }

    @Override
    public int getStartPosition() throws ControlWindowException {

        if(isFromBegin()){
            if(this instanceof CycleWindow){
                dataFlow.setSplitStartTime(dataFlow.getDataList().get(0).getTimestamp());
                dataFlow.setByPoint(true);
            }
            return 0;
        }

        Range ptr = prepareStart();
        if (!isCanWindowStartPositionBeDetermined(ptr, getAttachedDataFlow(), isCanWindowEndBeDetermined())) {
            return -1;
        }
        try {
            if(direction_start=='b'){
                if((this instanceof CycleWindow)||(this instanceof StepIdBasedWindow)||(this instanceof RSNStepIdBasedWindow)){
                    dataFlow.setSplitStartTime(dataFlow.getDataList().get(ptr.start).getTimestamp());
                    dataFlow.setByPoint(true);
                }else{
                    if(ptr.start==0){
                        if(unit_start=='p'){
                            dataFlow.setByPoint(true);
                            dataFlow.setSplitStartTime(dataFlow.getDataList().get(ptr.start).getTimestamp());
                        }else if(unit_start=='S'||unit_start=='s'){
                            dataFlow.setSplitStartTime(dataFlow.getStartTime());
                        }

                    }else{
                        dataFlow.setSplitStartTime(dataFlow.getDataList().get(ptr.start).getTimestamp());
                    }
                }
            }else if(direction_start=='e'){
                if(ptr.end==dataFlow.getDataList().size()){
                    dataFlow.setSplitStartTime(dataFlow.getSplitStopTime());
                }else{
                    dataFlow.setSplitStartTime(dataFlow.getDataList().get(ptr.end).getTimestamp());
                }
            }

        }catch (Throwable t){
            //TODO:
//            t.printStackTrace();
        }
        TimeOffset timeOffset = new TimeOffset(direction_start,value_start,unit_start);
        timeOffset.setEndPart(false);
        try {
            return timeOffset.eval(getAttachedDataFlow(),ptr);
        } catch (Throwable throwable) {
            if(throwable instanceof ControlWindowException){
                throw (ControlWindowException)throwable;
            }else{
                throw new ControlWindowException(throwable.getMessage(),throwable);
            }
        }
    }

    @Override
    public int getEndPosition() throws ControlWindowException {
        if(isToEnd()){
            return getAttachedDataFlow().getDataList().size();
        }
        Range ptr = prepareEnd();
        if (!isCanWindowEndPositionBeDetermined(ptr, getAttachedDataFlow(), isCanWindowEndBeDetermined())) {
            return -1;
        }
        try {
            if(direction_end=='e'){
                if(ptr.end==dataFlow.getDataList().size()){
                    dataFlow.setSplitStopTime(dataFlow.getSplitStopTime());
                }else{
                    dataFlow.setSplitStopTime(dataFlow.getDataList().get(ptr.end).getTimestamp());
                }
            }else if(direction_end=='b'){
                if(ptr.start==0){
                    dataFlow.setSplitStopTime(dataFlow.getStartTime());
                }else{
                    dataFlow.setSplitStopTime(dataFlow.getDataList().get(ptr.start).getTimestamp());
                }
            }

        }catch (Throwable t){
            //TODO://
            t.printStackTrace();
        }
        try {
            if(direction_end=='b'){
                if(unit_end=='p'){
                    return new TimeOffset(direction_end,value_end,unit_end).setEndPart(true).eval(getAttachedDataFlow(),ptr)+1;
                }else{
                    return new TimeOffset(direction_end,value_end,unit_end).setEndPart(true).eval(getAttachedDataFlow(),ptr);
                }
            }else{
                return new TimeOffset(direction_end,value_end,unit_end).setEndPart(true).eval(getAttachedDataFlow(),ptr);
            }

        } catch (Throwable throwable) {
            if(throwable instanceof ControlWindowException){
                throw (ControlWindowException)throwable;
            }else{
                throw new ControlWindowException(throwable.getMessage(),throwable);
            }
        }
    }


    public char getUnit_start() {
        return unit_start;
    }

    public void setUnit_start(char unit_start) {
        this.unit_start = unit_start;
    }

    public char getUnit_end() {
        return unit_end;
    }

    public void setUnit_end(char unit_end) {
        this.unit_end = unit_end;
    }

    public char getDirection_start() {
        return direction_start;
    }

    public void setDirection_start(char direction_start) {
        this.direction_start = direction_start;
    }

    public char getDirection_end() {
        return direction_end;
    }

    public void setDirection_end(char direction_end) {
        this.direction_end = direction_end;
    }

    public int getValue_start() {
        return value_start;
    }

    public void setValue_start(int value_start) {
        this.value_start = value_start;
    }

    public int getValue_end() {
        return value_end;
    }

    public void setValue_end(int value_end) {
        this.value_end = value_end;
    }

    public boolean isCanWindowStartPositionBeDetermined(Range startRange, IDataFlow dataFlow, boolean runEndFlag) {
        // 判断区间是否是有效区间
        if(startRange.start==null || startRange.end==null){
            return false;
        }

        if(direction_start=='b'){
            // 判断起始时间或起始位置是否在流的范围内
            int size = dataFlow.getDataList().size();
            long lastPointTime = dataFlow.getDataList().get(size - 1).getTimestamp();
            if ('s' == unit_start) {
                long windowStartTime = dataFlow.getDataList().get(startRange.start).getTimestamp() + value_start * 1000L;
                if (windowStartTime <= lastPointTime) {
                    return true;
                }
            } else if ('S' == unit_start) {
                long windowStartTime = dataFlow.getDataList().get(startRange.start).getTimestamp() + value_start;
                if (windowStartTime <= lastPointTime) {
                    return true;
                }
            } else if ('p' == unit_start) {
                int startPos = startRange.start + value_start;
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

    public boolean isCanWindowEndPositionBeDetermined(Range endRange, IDataFlow dataFlow, boolean runEndFlag) {
        // 判断区间是否是有效区间
        if(endRange.start==null || endRange.end==null){
            return false;
        }

        if(direction_end=='b'){
            // 判断结束时间或结束位置是否在流的范围内
            int size = dataFlow.getDataList().size();
            long lastPointTime = dataFlow.getDataList().get(size - 1).getTimestamp();
            if ('s' == unit_end) {
                long windowEndTime = dataFlow.getDataList().get(endRange.start).getTimestamp() + value_end * 1000L;
                if (windowEndTime < lastPointTime) {
                    return true;
                }
            } else if ('S' == unit_end) {
                long windowEndTime = dataFlow.getDataList().get(endRange.start).getTimestamp() + value_end;
                if (windowEndTime < lastPointTime) {
                    return true;
                }
            } else if ('p' == unit_end) {
                int lastPos = endRange.start + value_end;
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
    public String displayStartExp() {
        NumberFormat nf = NumberFormat.getNumberInstance();
        nf.setMaximumFractionDigits(3);
        StringBuilder sb = new StringBuilder();
        sb.append("T").append(direction_start);
        if(unit_start=='S'){
            sb.append(nf.format(value_start/1000.0));
        }else{
            sb.append(value_start);
        }
        sb.append("s");
        return sb.toString();
    }

    @Override
    public String displayEndExp() {
        NumberFormat nf = NumberFormat.getNumberInstance();
        nf.setMaximumFractionDigits(3);
        StringBuilder sb = new StringBuilder();
        sb.append("T").append(direction_end);
        if(unit_start=='S'){
            sb.append(nf.format(value_end/1000.0));
        }else{
            sb.append(value_end);
        }
        sb.append("s");
        return sb.toString();
    }
}
