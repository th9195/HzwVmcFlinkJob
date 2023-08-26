package com.hzw.fdc.bean.po;

import lombok.Data;
import lombok.experimental.Accessors;

/**
 * <p>描述</p>
 *
 * @author liuwentao
 * @date 2021/4/21 11:06
 */
@Data
@Accessors(chain = true)
public class WindowConfigPO {

    private Long contextId;
    private Long controlWindowId;
    private Long parentWindowId;
    private String controlWindowType;
    private Boolean isTopWindows;
    private Boolean isConfiguredIndicator;
    private String windowStart;
    private String windowEnd;
    private Long controlPlanId;
    private Long controlPlanVersion;
    private Long sensorAliasId;
    private String sensorAliasName;
    private Long indicatorId;
    private String windowExt;
    private String calculationTrigger;

    public Long getContextId() {
        return contextId;
    }

    public Long getControlWindowId() {
        return controlWindowId;
    }

    public Long getParentWindowId() {
        return parentWindowId;
    }

    public String getControlWindowType() {
        return controlWindowType;
    }

    public Boolean getTopWindows() {
        return isTopWindows;
    }

    public Boolean getConfiguredIndicator() {
        return isConfiguredIndicator;
    }

    public String getWindowStart() {
        return windowStart;
    }

    public String getWindowEnd() {
        return windowEnd;
    }

    public Long getControlPlanId() {
        return controlPlanId;
    }

    public Long getControlPlanVersion() {
        return controlPlanVersion;
    }

    public Long getSensorAliasId() {
        return sensorAliasId;
    }

    public String getSensorAliasName() {
        return sensorAliasName;
    }

    public Long getIndicatorId() {
        return indicatorId;
    }

    public String getWindowExt() {
        return windowExt;
    }

    public String getCalculationTrigger() {
        return calculationTrigger;
    }
}
