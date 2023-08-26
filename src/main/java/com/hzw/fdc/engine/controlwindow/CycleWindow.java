package com.hzw.fdc.engine.controlwindow;

import com.hzw.fdc.engine.controlwindow.cycle.CycleUnitPtr;
import com.hzw.fdc.engine.controlwindow.cycle.CycleUnitUtil;
import com.hzw.fdc.engine.controlwindow.data.defs.IDataFlow;
import com.hzw.fdc.engine.controlwindow.data.defs.IDataPacket;
import com.hzw.fdc.engine.controlwindow.data.defs.IIndicatorData;
import com.hzw.fdc.engine.controlwindow.data.impl.*;
import com.hzw.fdc.engine.controlwindow.exception.ControlWindowException;
import com.hzw.fdc.engine.controlwindow.exception.ControlWindowExpressionError;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CycleWindow extends TimeBasedWindow implements Serializable {

    private static final long serialVersionUID = 1552148753862269504L;
    private String pattern="(?,*)";
    private boolean maxMode = false;

    //性质：状态变量
    private List<CycleUnitPtr> cycleUnitPtrList = new ArrayList<>();

    //性质：状态变量
    private String stepIdSequence="";

    List<DataFlow.StepPtr> stepPtrList = null;

    //性质：状态变量
    private int currentStart = 0;

    //性质：状态变量
    private int currentEnd = 0;

    //性质：状态变量
    private boolean isInPrepareCycUnitMode = true;

    //性质：状态变量
    private IDataFlow backupDataFlow = null;

    public CycleWindow(String pattern, boolean maxMode, String start, String end) throws Exception {
        this.pattern = pattern;
        this.maxMode = maxMode;
        super.startExp = start;
        super.endExp = end;
        parse(start,end);
    }


    private void parse(String start, String end) throws  Exception{
        try {
            Pattern pattern = Pattern.compile("(T)([be])(\\d+)([psS]){1}");

            if (PRE_START.equals(start)) {
                setFromBegin(true);
            } else {
                setFromBegin(false);
                Matcher matcher = pattern.matcher(start);
                if (matcher.find()) {

                    setDirection_start(matcher.group(2).charAt(0));
                    setValue_start(Integer.parseInt(matcher.group(3)));
                    setUnit_start(matcher.group(4).charAt(0));

                } else {
                    throwException("TimeBasedWindow表达式错误", start, end, null);
                }
            }
            if (ControlWindow.PRE_END.equals(end)) {
                setToEnd(true);
            } else {
                setToEnd(false);
                Matcher matcher = pattern.matcher(end);
                if (matcher.find()) {

                    setDirection_end(matcher.group(2).charAt(0));
                    setValue_end(Integer.parseInt(matcher.group(3)));
                    setUnit_end(matcher.group(4).charAt(0));

                } else {
                    throwException("TimeBasedWindow表达式错误", start, end, null);
                }
            }
        } catch (Throwable t) {
            throw new ControlWindowExpressionError("TimeBasedWindow表达式错误:start=" + start + ",end=" + end, t);
        }
    }

    private void throwException(String msgPrefix, String start, String end, Throwable t) throws ControlWindowExpressionError {

        throw new ControlWindowExpressionError(msgPrefix + ":start=" + start + ",end=" + end, t);
    }


    @Override
    public int getStartPosition() throws ControlWindowException {
        dataFlow.setByPoint(true);
        if(isInPrepareCycUnitMode){
            return currentStart;
        }else{
            return super.getStartPosition();
        }

    }

    @Override
    public int getEndPosition() throws ControlWindowException {
        if(isInPrepareCycUnitMode){
            return currentEnd;
        }else{
            return super.getEndPosition();
        }
    }


    private int getRangStart(Range range){
        return range.start;
    }

    private int getRangeEnd(Range range){
        return range.end;
    }

    @Override
    public void reset() {
        super.reset();
        isInPrepareCycUnitMode = true;
//        backupDataFlow = null;
        stepIdSequence="";
        cycleUnitPtrList = new ArrayList<>();
    }

    @Override
    public List<WindowClipResponse.WindowSplitInfo> splitWindow() throws ControlWindowException {
        List<WindowClipResponse.WindowSplitInfo> result = new ArrayList<>();
        isInPrepareCycUnitMode = true;
        backupDataFlow();

        prepareStepIdSequence();
        try {
            calculateCycUnits();
        } catch (Throwable throwable) {
            throw new ControlWindowException("CycleUnitExtractionError: "+windowId+",["+pattern+"]",throwable);
        }

        List<Range> rangeList = new ArrayList<>();
        for(CycleUnitPtr cycleUnitPtr: cycleUnitPtrList){
            try{
                int startStepId = cycleUnitPtr.startIndex;
                int endStepId = cycleUnitPtr.endIndex;
                List<DataFlow.StepPtr> bs = backupDataFlow.getStepIds();
                List<DataFlow.StepPtr> tp = bs.subList(startStepId,endStepId);
                if(!tp.isEmpty()){
                    int startStepIdStartPosition = getRangStart(tp.get(0).range);
                    int endStepIdEndPosition = getRangeEnd(tp.get(tp.size()-1).range);
                    Range range = new Range(startStepIdStartPosition,endStepIdEndPosition);
                    rangeList.add(range);
                }

            }catch (Throwable t){
                throw new ControlWindowException("[CycleUnitPtr][error]:"+cycleUnitPtr,t);
            }
        }

        for(Range range: rangeList) {
            currentStart = range.start;
            currentEnd = range.end;

            // 判断是否可以进行window的切分
            if (!isCanWindowStartPositionBeDetermined(range, backupDataFlow, super.isCanWindowEndBeDetermined())
                    || !isCanWindowEndPositionBeDetermined(range, backupDataFlow, super.isCanWindowEndBeDetermined())) {
                continue;
            }

            try{
                IDataFlow cycleUnitDataFlow = backupDataFlow.getSubDataList(range.start,range.end);
                selfAttachDataFlow(cycleUnitDataFlow,true,true);
                isInPrepareCycUnitMode = false;
                List<WindowClipResponse.WindowSplitInfo> list = super.splitWindow();
                result.addAll(list);
                isInPrepareCycUnitMode = true;
            }catch (Throwable t){
                t.printStackTrace();
            }
        }

        selfAttachDataFlow(backupDataFlow,true,true);
        return result;
    }

    @Override
    public List<IIndicatorData> calculateIndicatorData() throws ControlWindowException {

        List<IIndicatorData> result = new ArrayList<>();
        isInPrepareCycUnitMode = true;
        backupDataFlow();

        prepareStepIdSequence();
        try {
            calculateCycUnits();
        } catch (Throwable throwable) {
            throw new ControlWindowException("CycleUnitExtractionError: "+windowId+",["+pattern+"]",throwable);
        }

        List<Range> rangeList = new ArrayList<>();
        int fromBegin = 0;

        List<DataFlow.StepPtr> stepPtrList =  backupDataFlow.getStepIds();
        for(CycleUnitPtr cycleUnitPtr: cycleUnitPtrList){
            try{
                int startStepId = cycleUnitPtr.startIndex;
                int endStepId = cycleUnitPtr.endIndex;
                List<DataFlow.StepPtr> bs = backupDataFlow.getStepIds();
                List<DataFlow.StepPtr> tp = bs.subList(startStepId,endStepId);
                if(!tp.isEmpty()){
                    int startStepIdStartPosition = getRangStart(tp.get(0).range);
                    int endStepIdEndPosition = getRangeEnd(tp.get(tp.size()-1).range);
                    Range range = new Range(startStepIdStartPosition,endStepIdEndPosition);
                    rangeList.add(range);
                    fromBegin = endStepIdEndPosition;
                }

            }catch (Throwable t){
                throw new ControlWindowException("[CycleUnitPtr][error]:"+cycleUnitPtr,t);
            }
        }


        int i = 1;
        for(Range range: rangeList){
            currentStart = range.start;
            currentEnd = range.end;

            // 判断是否可以进行window的切分
            if (!isCanWindowStartPositionBeDetermined(range, backupDataFlow, super.isCanWindowEndBeDetermined())
                    || !isCanWindowEndPositionBeDetermined(range, backupDataFlow, super.isCanWindowEndBeDetermined())) {
                continue;
            }


            try{
                IDataFlow cycleUnitDataFlow = backupDataFlow.getSubDataList(range.start,range.end);
                selfAttachDataFlow(cycleUnitDataFlow,true,true);
                isInPrepareCycUnitMode = false;
                List<IIndicatorData> list = super.calculateIndicatorData();
                for(IIndicatorData data: list){
                    ((IndicatorData)data).setCycleUnitIndex(i);
                }
                result.addAll(list);
                isInPrepareCycUnitMode = true;
                i++;
            }catch (Throwable t){
                t.printStackTrace();
                System.out.println("exception...");
            }
        }

        selfAttachDataFlow(backupDataFlow,true,true);
        return result;
    }


    private void backupDataFlow() {
        backupDataFlow = DataFlow.build();
        IDataFlow dataFlow = getAttachedDataFlow();
        for(IDataPacket dataPacket: dataFlow.getDataList()){
            backupDataFlow.getDataList().add(dataPacket);
        }
        backupDataFlow.setSorted(dataFlow.isSorted());
        backupDataFlow.setStartTime(dataFlow.getStartTime());
        backupDataFlow.setStopTime(dataFlow.getStopTime());
    }

    private void prepareStepIdSequence(){
        this.stepPtrList = getAttachedDataFlow().getStepIds();
        StringBuilder sb = new StringBuilder();
        for(DataFlow.StepPtr stepPtr: stepPtrList){
            sb.append(stepPtr.stepId).append(",");
        }
        if((sb.length()>0)&&(sb.charAt(sb.length()-1)==',')){
            sb.deleteCharAt(sb.length()-1);
        }
        stepIdSequence = sb.toString();
    }
    private void calculateCycUnits() throws Throwable {
        cycleUnitPtrList = CycleUnitUtil.extractCycleUnit(pattern,stepIdSequence,maxMode);
    }

    public String getPattern() {
        return pattern;
    }

    public void setPattern(String pattern) {
        this.pattern = pattern;
    }

    public boolean isMaxMode() {
        return maxMode;
    }

    public void setMaxMode(boolean maxMode) {
        this.maxMode = maxMode;
    }

    public List<CycleUnitPtr> getCycleUnitPtrList() {
        return cycleUnitPtrList;
    }

    public void setCycleUnitPtrList(List<CycleUnitPtr> cycleUnitPtrList) {
        this.cycleUnitPtrList = cycleUnitPtrList;
    }

    public String getStepIdSequence() {
        return stepIdSequence;
    }

    public void setStepIdSequence(String stepIdSequence) {
        this.stepIdSequence = stepIdSequence;
    }

    public int getCurrentStart() {
        return currentStart;
    }

    public void setCurrentStart(int currentStart) {
        this.currentStart = currentStart;
    }

    public int getCurrentEnd() {
        return currentEnd;
    }

    public void setCurrentEnd(int currentEnd) {
        this.currentEnd = currentEnd;
    }

    public boolean isInPrepareCycUnitMode() {
        return isInPrepareCycUnitMode;
    }

    public void setInPrepareCycUnitMode(boolean inPrepareCycUnitMode) {
        isInPrepareCycUnitMode = inPrepareCycUnitMode;
    }

    public IDataFlow getBackupDataFlow() {
        return backupDataFlow;
    }

    public void setBackupDataFlow(IDataFlow backupDataFlow) {
        this.backupDataFlow = backupDataFlow;
    }

    @Override
    public boolean isCanWindowEndBeDetermined() {
        return true;
    }
}
