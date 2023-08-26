package com.hzw.fdc.wafern.type;

import lombok.Data;

import java.io.Serializable;


public class BypassCondition implements Serializable {
    String condition;
    Integer recentRuns;

    public String getCondition() {
        return condition;
    }

    public void setCondition(String condition) {
        this.condition = condition;
    }

    public Integer getRecentRuns() {
        return recentRuns;
    }

    public void setRecentRuns(Integer recentRuns) {
        this.recentRuns = recentRuns;
    }
}