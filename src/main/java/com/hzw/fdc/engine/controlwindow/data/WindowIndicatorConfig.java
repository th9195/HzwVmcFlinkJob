package com.hzw.fdc.engine.controlwindow.data;

import java.util.Objects;

public class WindowIndicatorConfig {
    public Long indicatorId;
    public Long sensorAliasId;
    public String sensorAliasName;
    public boolean debug;

    public WindowIndicatorConfig() { }

    public WindowIndicatorConfig(Long indicatorId, Long sensorAliasId, String sensorAliasName) {
        this.indicatorId = indicatorId;
        this.sensorAliasId = sensorAliasId;
        this.sensorAliasName = sensorAliasName;
    }

    @Override
    public String toString() {
        return "WindowIndicatorConfig{" +
                "indicatorId=" + indicatorId +
                ", sensorAliasId="+sensorAliasId+
                ", sensorAliasName=" + sensorAliasName +
                '}';
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WindowIndicatorConfig that = (WindowIndicatorConfig) o;
        return Objects.equals(indicatorId, that.indicatorId) &&
                Objects.equals(sensorAliasId, that.sensorAliasId) &&
                Objects.equals(sensorAliasName, that.sensorAliasName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indicatorId, sensorAliasId, sensorAliasName);
    }
}
