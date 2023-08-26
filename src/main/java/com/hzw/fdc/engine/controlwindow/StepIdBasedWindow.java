package com.hzw.fdc.engine.controlwindow;

import com.hzw.fdc.engine.controlwindow.data.defs.IDataPacket;
import com.hzw.fdc.engine.controlwindow.data.impl.Range;

import java.io.Serializable;
import java.util.List;

public class StepIdBasedWindow extends TimeBasedWindow implements Serializable {

    private static final long serialVersionUID = -3127904889594162286L;
    private int stepId_start = 0;
    private int stepId_end = 0;

    public StepIdBasedWindow() {
    }

    public StepIdBasedWindow(Long id, int stepId_start, char unit_start, char direction_start, int value_start, int stepId_end, char unit_end, char direction_end, int value_end) {
        super(id, unit_start, direction_start, value_start, unit_end, direction_end, value_end);
        this.stepId_start = stepId_start;
        this.stepId_end = stepId_end;
    }

    public int getStepId_start() {
        return stepId_start;
    }

    public void setStepId_start(int stepId_start) {
        this.stepId_start = stepId_start;
    }

    public int getStepId_end() {
        return stepId_end;
    }

    public void setStepId_end(int stepId_end) {
        this.stepId_end = stepId_end;
    }

    protected Range prepareStartCycle(){
        return new Range(0,getAttachedDataFlow().getDataList().size());
    }

    protected Range prepareEndCycle(){
        return new Range(0,getAttachedDataFlow().getDataList().size());
    }

    @Override
    protected Range prepareStart() {
        Range rsn = prepareStartCycle();
        List<IDataPacket> list = getAttachedDataFlow().getDataList();
        boolean isFound = false;
        Range ptr = new Range();
        int stepId = stepId_start;
        for(int i=rsn.start;i<rsn.end;i++){
            if(!isFound){
                if(list.get(i).getStepId()==stepId){
                    ptr.start = i;
                    isFound = true;
                }
            }else{
                if(list.get(i).getStepId()!=stepId){
                    ptr.end = i;
                    break;
                }else if(i==(rsn.end-1)){
                    ptr.end = rsn.end;
                    break;
                }
            }
        }

        if(ptr.end==null){
            ptr.end = list.size();
        }

        return ptr;
    }

    @Override
    protected Range prepareEnd() {
        Range rsn = prepareEndCycle();
        List<IDataPacket> list = getAttachedDataFlow().getDataList();
        boolean isFound = false;
        Range ptr = new Range();
        int stepId = stepId_end;
        for(int i=rsn.start;i<=rsn.end;i++){
            if(!isFound){
                if(list.get(i).getStepId()==stepId){
                    ptr.start = i;
                    isFound = true;
                }
            }else{
                if(i==rsn.end){
                    ptr.end = i;
                }else if(i==(rsn.end-1)){
                    if((list.get(i).getStepId()!=stepId)){
                        ptr.end = i;
                        break;
                    }
                }else {
                    if((list.get(i).getStepId()!=stepId)){
                        ptr.end = i;
                        break;
                    }
                }

            }
        }
        if(ptr.end==null){
            ptr.end = list.size();
        }
        if(ptr.end<list.size()){
//            dataFlow.setStopTime(list.get(ptr.end).getTimestamp());
        }
        return ptr;
    }

}
