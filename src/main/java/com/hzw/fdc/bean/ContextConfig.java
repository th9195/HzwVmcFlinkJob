package com.hzw.fdc.bean;

import lombok.Data;
import lombok.experimental.Accessors;

/**
 * @author lwt
 */
@Data
@Accessors(chain = true)
public class ContextConfig {

    private Long locationId;
    private String locationName;
    private Long moduleId;
    private String moduleName;
    private String toolName;
    private String chamberName;
    private String recipeName;
    private String productName;
    private String stage;
    private Long contextId;
    private Long toolGroupId;
    private String toolGroupName;
    private Long toolGroupVersion;
    private Long chamberGroupId;
    private String chamberGroupName;
    private Long chamberGroupVersion;
    private Long recipeGroupId;
    private String recipeGroupName;
    private Long recipeGroupVersion;
    private Boolean limitStatus;
    private String area;
    private String section;
    private String mesChamberName;

    public String getArea() {
        return area;
    }

    public String getSection() {
        return section;
    }

    public String getMesChamberName() {
        return mesChamberName;
    }

    public Long getLocationId() {
        return locationId;
    }

    public String getLocationName() {
        return locationName;
    }

    public Long getModuleId() {
        return moduleId;
    }

    public String getModuleName() {
        return moduleName;
    }

    public String getToolName() {
        return toolName;
    }

    public String getChamberName() {
        return chamberName;
    }

    public String getRecipeName() {
        return recipeName;
    }

    public String getProductName() {
        return productName;
    }

    public String getStage() {
        return stage;
    }

    public Long getContextId() {
        return contextId;
    }

    public Long getToolGroupId() {
        return toolGroupId;
    }

    public String getToolGroupName() {
        return toolGroupName;
    }

    public Long getToolGroupVersion() {
        return toolGroupVersion;
    }

    public Long getChamberGroupId() {
        return chamberGroupId;
    }

    public String getChamberGroupName() {
        return chamberGroupName;
    }

    public Long getChamberGroupVersion() {
        return chamberGroupVersion;
    }

    public Long getRecipeGroupId() {
        return recipeGroupId;
    }

    public String getRecipeGroupName() {
        return recipeGroupName;
    }

    public Long getRecipeGroupVersion() {
        return recipeGroupVersion;
    }

    public Boolean getLimitStatus() {
        return limitStatus;
    }
}
