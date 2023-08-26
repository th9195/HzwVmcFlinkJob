package com.hzw.fdc.engine.controlwindow.cycle;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class CycleUnitPtr implements Serializable {
    private static final long serialVersionUID = 986984390366348451L;
    public int startIndex;
    public int endIndex;
    public String cycleUnitStr;
    public List<Integer> steps = new ArrayList<>();

    @Override
    public String toString() {
        return new StringBuilder().append("CycleUnitPtr{").append("start=").append(startIndex).append(", end=").append(endIndex).append(", cycleUnitStr='").append(cycleUnitStr).append('\'').append('}').toString();
    }
}
