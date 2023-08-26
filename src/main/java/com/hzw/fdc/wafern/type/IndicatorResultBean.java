package com.hzw.fdc.wafern.type;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 *  indicatorId + toolName + chamberName
 */
public class IndicatorResultBean implements Serializable {
    public static final String PM_END_TAG = "end";
    public static final String PM_START_TAG = "start";
    Long controlPlanId;
    String controlPlanName;
    Long controlPlanVersion;
    Long locationId;
    String locationName;
    Long moduleId;
    String moduleName;
    Long toolGroupId;
    String toolGroupName;
    Long chamberGroupId;
    String chamberGroupName;
    String runId;
    String toolName;
    String chamberName;
    String indicatorValue;
    Long indicatorId;
    String indicatorName;
    Long indicatorCreateTime;
    Double missingRation;
    Double configMissingRation;
    Long runStartTime;
    Long runEndTime;
    Long windowStartTime;
    Long windowEndTime;
    Long windowDataCreateTime;
    Long limitStatus;
    String materialName;
    String recipeName;
    List<String> productName;
    List<String> stage;
    List<BypassCondition> bypassCondition;
    String PMStatus;
    Long PMTimestamp;

    public boolean isPMStatusEndMatch(){
        return PM_END_TAG.equals(PMStatus);
    }

    public boolean isPMStatusStartMatch(){
        return PM_START_TAG.equals(PMStatus);
    }

    public String getStandardizedProduct(){
        if(productName==null){
            return null;
        }else{
            List<String> tmp = new ArrayList<>(productName);
            Collections.sort(tmp);
            return tmp.toString();
        }
    }

    public String getStandardizedRecipe(){
       return recipeName;
    }


    public Long getControlPlanId() {
        return controlPlanId;
    }

    public void setControlPlanId(Long controlPlanId) {
        this.controlPlanId = controlPlanId;
    }

    public String getControlPlanName() {
        return controlPlanName;
    }

    public void setControlPlanName(String controlPlanName) {
        this.controlPlanName = controlPlanName;
    }

    public Long getControlPlanVersion() {
        return controlPlanVersion;
    }

    public void setControlPlanVersion(Long controlPlanVersion) {
        this.controlPlanVersion = controlPlanVersion;
    }

    public Long getLocationId() {
        return locationId;
    }

    public void setLocationId(Long locationId) {
        this.locationId = locationId;
    }

    public String getLocationName() {
        return locationName;
    }

    public void setLocationName(String locationName) {
        this.locationName = locationName;
    }

    public Long getModuleId() {
        return moduleId;
    }

    public void setModuleId(Long moduleId) {
        this.moduleId = moduleId;
    }

    public String getModuleName() {
        return moduleName;
    }

    public void setModuleName(String moduleName) {
        this.moduleName = moduleName;
    }

    public Long getToolGroupId() {
        return toolGroupId;
    }

    public void setToolGroupId(Long toolGroupId) {
        this.toolGroupId = toolGroupId;
    }

    public String getToolGroupName() {
        return toolGroupName;
    }

    public void setToolGroupName(String toolGroupName) {
        this.toolGroupName = toolGroupName;
    }

    public Long getChamberGroupId() {
        return chamberGroupId;
    }

    public void setChamberGroupId(Long chamberGroupId) {
        this.chamberGroupId = chamberGroupId;
    }

    public String getChamberGroupName() {
        return chamberGroupName;
    }

    public void setChamberGroupName(String chamberGroupName) {
        this.chamberGroupName = chamberGroupName;
    }

    public String getRunId() {
        return runId;
    }

    public void setRunId(String runId) {
        this.runId = runId;
    }

    public String getToolName() {
        return toolName;
    }

    public void setToolName(String toolName) {
        this.toolName = toolName;
    }

    public String getChamberName() {
        return chamberName;
    }

    public void setChamberName(String chamberName) {
        this.chamberName = chamberName;
    }

    public String getIndicatorValue() {
        return indicatorValue;
    }

    public void setIndicatorValue(String indicatorValue) {
        this.indicatorValue = indicatorValue;
    }

    public Long getIndicatorId() {
        return indicatorId;
    }

    public void setIndicatorId(Long indicatorId) {
        this.indicatorId = indicatorId;
    }

    public String getIndicatorName() {
        return indicatorName;
    }

    public void setIndicatorName(String indicatorName) {
        this.indicatorName = indicatorName;
    }

    public Long getIndicatorCreateTime() {
        return indicatorCreateTime;
    }

    public void setIndicatorCreateTime(Long indicatorCreateTime) {
        this.indicatorCreateTime = indicatorCreateTime;
    }

    public Double getMissingRation() {
        return missingRation;
    }

    public void setMissingRation(Double missingRation) {
        this.missingRation = missingRation;
    }

    public Double getConfigMissingRation() {
        return configMissingRation;
    }

    public void setConfigMissingRation(Double configMissingRation) {
        this.configMissingRation = configMissingRation;
    }

    public Long getRunStartTime() {
        return runStartTime;
    }

    public void setRunStartTime(Long runStartTime) {
        this.runStartTime = runStartTime;
    }

    public Long getRunEndTime() {
        return runEndTime;
    }

    public void setRunEndTime(Long runEndTime) {
        this.runEndTime = runEndTime;
    }

    public Long getWindowStartTime() {
        return windowStartTime;
    }

    public void setWindowStartTime(Long windowStartTime) {
        this.windowStartTime = windowStartTime;
    }

    public Long getWindowEndTime() {
        return windowEndTime;
    }

    public void setWindowEndTime(Long windowEndTime) {
        this.windowEndTime = windowEndTime;
    }

    public Long getWindowDataCreateTime() {
        return windowDataCreateTime;
    }

    public void setWindowDataCreateTime(Long windowDataCreateTime) {
        this.windowDataCreateTime = windowDataCreateTime;
    }

    public Long getLimitStatus() {
        return limitStatus;
    }

    public void setLimitStatus(Long limitStatus) {
        this.limitStatus = limitStatus;
    }

    public String getMaterialName() {
        return materialName;
    }

    public void setMaterialName(String materialName) {
        this.materialName = materialName;
    }

    public String getRecipeName() {
        return recipeName;
    }

    public void setRecipeName(String recipeName) {
        this.recipeName = recipeName;
    }

    public List<String> getProductName() {
        return productName;
    }

    public void setProductName(List<String> productName) {
        this.productName = productName;
    }

    public List<String> getStage() {
        return stage;
    }

    public void setStage(List<String> stage) {
        this.stage = stage;
    }

    public List<BypassCondition> getBypassCondition() {
        return bypassCondition;
    }

    public void setBypassCondition(List<BypassCondition> bypassCondition) {
        this.bypassCondition = bypassCondition;
    }

    public String getPMStatus() {
        return PMStatus;
    }

    public void setPMStatus(String PMStatus) {
        this.PMStatus = PMStatus;
    }

    public Long getPMTimestamp() {
        return PMTimestamp;
    }

    public void setPMTimestamp(Long PMTimestamp) {
        this.PMTimestamp = PMTimestamp;
    }
}
