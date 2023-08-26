package com.hzw.fdc.engine.controlwindow;

import com.hzw.fdc.engine.controlwindow.data.impl.DataFlow;
import com.hzw.fdc.engine.controlwindow.data.impl.Range;

import java.io.Serializable;
import java.util.List;

public class RSNStepIdBasedWindow extends StepIdBasedWindow implements Serializable {

    private static final long serialVersionUID = 7303070538023562243L;
    private int rsn_start;
    private int rsn_end;


    private List<DataFlow.RSNPtr> list = null;

    public RSNStepIdBasedWindow() {
    }

    public RSNStepIdBasedWindow(Long id, int rsn_start, int stepId_start, char unit_start, char direction_start, int value_start, int rsn_end, int stepId_end, char unit_end, char direction_end, int value_end) {
        super(id, stepId_start, unit_start, direction_start, value_start, stepId_end, unit_end, direction_end, value_end);
        this.rsn_start = rsn_start;
        this.rsn_end = rsn_end;
    }



    @Override
    protected Range prepareStartCycle() {
//        if(list==null){
//            list = getAttachedDataFlow().getRSNs();
//        }
        list = getAttachedDataFlow().getRSNs();
        DataFlow.RSNPtr rsnPtr = list.get(rsn_start-1);
        return rsnPtr.getRange();
    }

    @Override
    protected Range prepareEndCycle() {
//        if(list==null){
        list = getAttachedDataFlow().getRSNs();
//        }
        DataFlow.RSNPtr rsnPtr = list.get(rsn_end-1);

        return rsnPtr.getRange();
    }

    public int getRsn_start() {
        return rsn_start;
    }

    public void setRsn_start(int rsn_start) {
        this.rsn_start = rsn_start;
    }

    public int getRsn_end() {
        return rsn_end;
    }

    public void setRsn_end(int rsn_end) {
        this.rsn_end = rsn_end;
    }

    public List<DataFlow.RSNPtr> getList() {
        return list;
    }

    public void setList(List<DataFlow.RSNPtr> list) {
        this.list = list;
    }

}
