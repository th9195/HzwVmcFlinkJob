package com.hzw.fdc.wafern.type;

public enum WaferNConditionType {

    RecipeChanged("RecipeChanged"),
    ProductChanged("ProductChanged"),
    FirstRunAfterPM("1st Run After PM"),
    FirstProductAfterPM("1st Product After PM"),
    UNKNWON("unknown");

    String value;
    WaferNConditionType(String value) {
        this.value = value;
    }
    public String value(){
        return value;
    }


    public static WaferNConditionType parse(String test){
        if(test.equals(RecipeChanged.value())){
            return RecipeChanged;
        }else if(test.equals(ProductChanged.value())){
            return ProductChanged;
        }else if(test.equals(FirstProductAfterPM.value())){
            return FirstProductAfterPM;
        }else if(test.equals(FirstRunAfterPM.value())){
            return FirstRunAfterPM;
        }else{
            return UNKNWON;
        }
    }
}
