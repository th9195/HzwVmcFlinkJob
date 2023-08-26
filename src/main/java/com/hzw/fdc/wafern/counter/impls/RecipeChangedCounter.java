package com.hzw.fdc.wafern.counter.impls;

import com.hzw.fdc.wafern.counter.AbstractWaferNStatefulCounter;
import com.hzw.fdc.wafern.type.BypassCondition;
import com.hzw.fdc.wafern.type.IndicatorResultBean;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class RecipeChangedCounter extends AbstractWaferNStatefulCounter implements Serializable {

    Map<Long,String> indicatorRecipeMap = new HashMap<>();

    @Override
    public void process(IndicatorResultBean bean, BypassCondition bypassCondition) throws Throwable {
        super.process(bean,bypassCondition);
//        Long indicatorId = bean.getIndicatorId();
        Long indicatorId = indicatorCountKey(bean);
        String recipe = bean.getStandardizedRecipe();
        if(!indicatorCounter.containsKey(indicatorId)){ // 还没有记录过这个indicator的状态时
            assert !indicatorRecipeMap.containsKey(indicatorId);
            if(recipe==null){
                // 则忽略， 因为，之前也没有这个indicator的状态，而且当前indicator的recipe也是null，因此，不存在发生改变一说。
            }else{
                // 之前没有这个indicator的状态，但当前indicator的recipe有值了，说明是第一次有recipe的值，但不算发生了变化。
                // 因此，在indicatorRecipeMap中记录这个indicator的recipe
                indicatorCounter.put(indicatorId,1); // 代表这个indicator的recipe有值了，[经过确认，这个也算发生了变化]
                indicatorRecipeMap.put(indicatorId,recipe);

            }

        }else{
            // 还是0，则代表product有值，但没有发生变化，接下来变不变，要看当前indicator的值了
            assert indicatorRecipeMap.containsKey(indicatorId);
            if(justEquals(recipe,indicatorRecipeMap.get(indicatorId))&&(!isConditionChanged(bean,bypassCondition))){
                // 之前存储的indicator的recipe和现在来的indicator的recipe一致，没有发生改变，则啥也不处理
                indicatorCounter.put(indicatorId,indicatorCounter.get(indicatorId)+1);
            }else{
                // 之前存储的indicator的recipe和现在来的indicator的recipe不一致，发生了改变，则：
                indicatorCounter.put(indicatorId,1);    // 将计数器置为1
                indicatorRecipeMap.put(indicatorId,recipe);  // 将product记录表中的recipe记录为当前indicator的recipe
            }
        }


    }
}
