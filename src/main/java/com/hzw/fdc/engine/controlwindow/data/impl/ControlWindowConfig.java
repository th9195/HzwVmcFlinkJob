package com.hzw.fdc.engine.controlwindow.data.impl;

import com.hzw.fdc.engine.controlwindow.data.WindowIndicatorConfig;

import java.util.List;

public class ControlWindowConfig {

    private String type;
    private String start;
    private String end;
    private List<WindowIndicatorConfig> indicatorConfigList;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getStart() {
        return start;
    }

    public void setStart(String start) {
        this.start = start;
    }

    public String getEnd() {
        return end;
    }

    public void setEnd(String end) {
        this.end = end;
    }

    public List<WindowIndicatorConfig> getIndicatorConfigList() {
        return indicatorConfigList;
    }

    public void setIndicatorConfigList(List<WindowIndicatorConfig> indicatorConfigList) {
        this.indicatorConfigList = indicatorConfigList;
    }
}
