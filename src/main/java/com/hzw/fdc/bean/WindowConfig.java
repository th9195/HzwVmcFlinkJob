package com.hzw.fdc.bean;

import lombok.Data;
import lombok.experimental.Accessors;

import java.util.List;

/**
 * <p>描述</p>
 *
 * @author liuwentao
 * @date 2021/4/21 11:06
 */
@Data
@Accessors(chain = true)
public class WindowConfig {

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
    private String calculationTrigger;
    private List<SensorAlias> sensorAlias;

    public Long getContextId() {
        return contextId;
    }

    public Long getControlWindowId() {
        return controlWindowId;
    }

    public Long getParentWindowId() {
        return parentWindowId;
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

    public List<SensorAlias> getSensorAlias() {
        return sensorAlias;
    }

    public String getControlWindowType() {
        return controlWindowType;
    }

    public String getCalculationTrigger() {
        return calculationTrigger;
    }

    @Data
    @Accessors(chain = true)
    public static class SensorAlias {

        private Long sensorAliasId;
        private String sensorAliasName;
        private List<Long> indicatorId;

        public Long getSensorAliasId() {
            return sensorAliasId;
        }

        public String getSensorAliasName() {
            return sensorAliasName;
        }

        public List<Long> getIndicatorId() {
            return indicatorId;
        }
    }

}
