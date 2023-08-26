package com.hzw.fdc.engine.controlwindow;

import java.util.ArrayList;
import java.util.List;

public class CycleUnitPtr {
    public int startIndex;
    public int endIndex;
    public String cycleUnitStr;
    public List<Integer> steps = new ArrayList<>();

    @Override
    public String toString() {
        return "CycleUnitPtr{" +
                "start=" + startIndex +
                ", end=" + endIndex +
                ", cycleUnitStr='" + cycleUnitStr + '\'' +
                '}';
    }
}
