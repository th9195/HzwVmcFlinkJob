package com.hzw.fdc.engine.controlwindow.data.impl;

import java.io.Serializable;

public class Range implements Serializable {
    private static final long serialVersionUID = -505424295183879787L;
    public Integer start;
    public Integer end;

    public Range(int start, int end) {
        this.start = start;
        this.end = end;
    }

    public Range() {
    }

    public int getStart() {
        return start;
    }

    public void setStart(int start) {
        this.start = start;
    }

    public int getEnd() {
        return end;
    }

    public void setEnd(int end) {
        this.end = end;
    }

    @Override
    public String toString() {
        return "["+start+","+end+")";
    }
}
