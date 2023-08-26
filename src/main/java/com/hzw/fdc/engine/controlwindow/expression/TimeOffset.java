package com.hzw.fdc.engine.controlwindow.expression;

import com.hzw.fdc.engine.controlwindow.data.defs.IDataFlow;
import com.hzw.fdc.engine.controlwindow.data.defs.IDataPacket;
import com.hzw.fdc.engine.controlwindow.data.impl.DataFlow;
import com.hzw.fdc.engine.controlwindow.data.impl.Range;
import com.hzw.fdc.engine.controlwindow.exception.ControlWindowException;
import com.hzw.fdc.engine.controlwindow.exception.ControlWindowExpressionError;

import java.io.Serializable;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TimeOffset implements Serializable {

    private static final long serialVersionUID = -2937124727321314536L;
    static Pattern pattern = Pattern.compile("(T)([be])(\\d+)([psS]){1}");

    public char unit;
    public char direction;
    public int value;
    private boolean isEndPart = false;

    public TimeOffset() {
    }

    public TimeOffset(char direction, int value, char unit ) {
        this.unit = unit;
        this.direction = direction;
        this.value = value;
    }

    public long getSplitTime(DataFlow dataFlow){
        if(isEndPart){
            return dataFlow.getSplitStopTime();
        }else{
            return dataFlow.getSplitStartTime();
        }
    }

    public void setSplitTime(DataFlow dataFlow,long time){
        if(isEndPart){
            dataFlow.setSplitStopTime(time);
        }else{
            dataFlow.setSplitStartTime(time);
        }
    }

    public void setAutoBound(DataFlow dataFlow,boolean autoBound){
        if(isEndPart){
            dataFlow.setAutoEndBound(autoBound);
        }else{
            dataFlow.setAutoStartBound(autoBound);
        }
    }



    public Integer eval(IDataFlow dataFlow1, Range ptr) throws Throwable{
        DataFlow dataFlow = (DataFlow) dataFlow1;
        if(unit=='s'){
            if(direction=='b'){
                List<IDataPacket> dataList = dataFlow.getDataList();
                long beginPointTimestamp = getSplitTime(dataFlow);
                long limitTimestamp = beginPointTimestamp + value*1000;
                setAutoBound(dataFlow,false);
                setSplitTime(dataFlow,limitTimestamp);

                int i=ptr.start;
                for(;i<dataList.size();i++){
                    if(dataList.get(i).getTimestamp()>=limitTimestamp){
                        if(isEndPart){
                            if(dataList.get(i).getTimestamp()==limitTimestamp){
                                return i+1;
                            }else{
                                return i;
                            }
                        }else{
                            return i;
                        }

                    }
                }
                return i;
            }else if(direction == 'e'){
                List<IDataPacket> dataList = dataFlow.getDataList();
                //原来是采用最后一个点作的时间作为比较时间。
                //现在改为用dataFlow的stoptime来作为时间点进行比较。
                long endPointTimestamp = getSplitTime(dataFlow);
                long limitTimestamp = endPointTimestamp - value*1000;
                setAutoBound(dataFlow,false);
                setSplitTime(dataFlow,limitTimestamp);

                int i=ptr.end-1;
                for(;i>=0;i--){
                    if(dataList.get(i).getTimestamp()<=limitTimestamp){
                        if(!isEndPart){
                            if(dataList.get(i).getTimestamp()==limitTimestamp){
                                return i;
                            }else{
                                return i+1;
                            }
                        }
                        break;
                    }
                }
                return i+1;
            }else{
                throw new ControlWindowException("不支持的方向类型");
            }

        }else if(unit=='S'){
            if(direction=='b'){
                List<IDataPacket> dataList = dataFlow.getDataList();
                long beginPointTimestamp = getSplitTime(dataFlow);
                long limitTimestamp = beginPointTimestamp + value;
                setAutoBound(dataFlow,false);
                setSplitTime(dataFlow,limitTimestamp);
                int i=ptr.start;
                for(;i<dataList.size();i++){
                    if(dataList.get(i).getTimestamp()>=limitTimestamp){
                        if(isEndPart){
                            if(dataList.get(i).getTimestamp()==limitTimestamp){
                                return i+1;
                            }else{
                                return i;
                            }
                        }else{
                            return i;
                        }
                    }
                }
                return i;
            }else if(direction == 'e'){
                List<IDataPacket> dataList = dataFlow.getDataList();
                long endPointTimestamp = getSplitTime(dataFlow);

                long limitTimestamp = endPointTimestamp - value;

                setAutoBound(dataFlow,false);
                setSplitTime(dataFlow,limitTimestamp);

                int i=ptr.end-1;
                for(;i>=0;i--){
                    if(dataList.get(i).getTimestamp()<=limitTimestamp){
                        if(!isEndPart){
                            if(dataList.get(i).getTimestamp()==limitTimestamp){
                                return i;
                            }else{
                                return i+1;
                            }
                        }
                        break;
                    }
                }
                return i+1;
            }else{
                throw new ControlWindowException("不支持的方向类型");
            }

        }
        else if(unit=='p'){
            if(direction=='b'){
                return ptr.start+value;
            }else if(direction=='e'){
                return ptr.end-value;
            }else{
                throw new ControlWindowException("不支持的方向类型");
            }
        }else{ // 不支持的单位类型
            throw new ControlWindowException("不支持的单位类型");
        }
    }

    public static TimeOffset parse(String exp) throws ControlWindowExpressionError{
        Matcher matcher = pattern.matcher(exp);
        if (matcher.find()) {
            TimeOffset timeOffset = new TimeOffset();
            timeOffset.direction = matcher.group(2).charAt(0);
            timeOffset.value = Integer.parseInt(matcher.group(3));
            timeOffset.unit = matcher.group(4).charAt(0);

            return timeOffset;
        } else {
            throw new ControlWindowExpressionError.TimeBasedControlWindowExpressionError("offset exp error:"+exp);
        }
    }


    public static Pattern getPattern() {
        return pattern;
    }

    public static void setPattern(Pattern pattern) {
        TimeOffset.pattern = pattern;
    }

    public char getUnit() {
        return unit;
    }

    public void setUnit(char unit) {
        this.unit = unit;
    }

    public char getDirection() {
        return direction;
    }

    public void setDirection(char direction) {
        this.direction = direction;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    public boolean isEndPart() {
        return isEndPart;
    }

    public TimeOffset setEndPart(boolean endPart) {
        isEndPart = endPart;
        return this;
    }
}

