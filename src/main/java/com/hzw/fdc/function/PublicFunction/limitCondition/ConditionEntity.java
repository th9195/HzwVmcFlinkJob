package com.hzw.fdc.function.PublicFunction.limitCondition;

import org.apache.commons.lang3.StringUtils;

public class ConditionEntity {

    @FieldOrder(0)
    String tool;

    @FieldOrder(1)
    String chamber;

    @FieldOrder(2)
    String recipe;

    @FieldOrder(3)
    String product;

    @FieldOrder(4)
    String stage;

    public ConditionEntity() {
    }

    public ConditionEntity(String tool, String chamber, String recipe, String product, String stage) {
        this.tool = tool;
        this.chamber = chamber;
        this.recipe = recipe;
        this.product = product;
        this.stage = stage;
    }

    public ConditionEntity join(ConditionEntity entity){
        if (entity==null) return this;
        ConditionEntity newCondition = new ConditionEntity();
        if(StringUtils.isNotBlank(entity.tool))    newCondition.tool    = this.tool    + "," + entity.tool;
        if(StringUtils.isNotBlank(entity.chamber)) newCondition.chamber = this.chamber + "," + entity.chamber;
        if(StringUtils.isNotBlank(entity.recipe))  newCondition.recipe  = this.recipe  + "," + entity.recipe;
        if(StringUtils.isNotBlank(entity.product)) newCondition.product = this.product + "," + entity.product;
        if(StringUtils.isNotBlank(entity.stage))   newCondition.stage   = this.stage   + "," + entity.stage;
        return newCondition;
    }

    @Override
    public String toString() {
        return "ConditionEntity{" +
                "tool='" + tool + '\'' +
                ", chamber='" + chamber + '\'' +
                ", recipe='" + recipe + '\'' +
                ", product='" + product + '\'' +
                ", stage='" + stage + '\'' +
                '}';
    }
}
