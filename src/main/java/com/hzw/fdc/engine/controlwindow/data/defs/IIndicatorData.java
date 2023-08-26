package com.hzw.fdc.engine.controlwindow.data.defs;

import java.util.List;

public interface IIndicatorData {
    public List<ISensorData> getData();
    public Long getIndicatorId();
    public Long getWindowId();
    public Long getSensorAliasId();
    public String getSensorAliasName();
    public Integer getCycleUnitIndex();

    public Long getStartTime();
    public void setStartTime(Long startTime);

    public Long getStopTime();
    public void setStopTime(Long stopTime);

    public WindowUnitPtr getParentWindowUnitPtr();

    public static class WindowUnitPtr {
        private Long windowId;
        private Integer cycleUnitIndex;

        public Long getWindowId() {
            return windowId;
        }

        public void setWindowId(Long windowId) {
            this.windowId = windowId;
        }

        public Integer getCycleUnitIndex() {
            return cycleUnitIndex;
        }

        public void setCycleUnitIndex(Integer cycleUnitIndex) {
            this.cycleUnitIndex = cycleUnitIndex;
        }
    }
}
