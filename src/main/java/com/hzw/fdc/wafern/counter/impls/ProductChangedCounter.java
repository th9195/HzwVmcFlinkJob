package com.hzw.fdc.wafern.counter.impls;

import com.hzw.fdc.wafern.counter.AbstractWaferNStatefulCounter;
import com.hzw.fdc.wafern.type.BypassCondition;
import com.hzw.fdc.wafern.type.IndicatorResultBean;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class ProductChangedCounter extends AbstractWaferNStatefulCounter implements Serializable {

    Map<Long,String> indicatorProductMap = new HashMap<>();

    @Override
    public void process(IndicatorResultBean bean, BypassCondition bypassCondition) throws Throwable {
        super.process(bean,bypassCondition);
//        Long indicatorId = bean.getIndicatorId();
        Long indicatorId = indicatorCountKey(bean);
        String products = bean.getStandardizedProduct();
        if(!indicatorCounter.containsKey(indicatorId)){ // 还没有记录过这个indicator的状态时
            assert !indicatorProductMap.containsKey(indicatorId);

            if(products==null){
                // 则忽略， 因为，之前也没有这个indicator的状态，而且当前indicator的product也是null，因此，不存在发生改变一说。
            }else{
                // 之前没有这个indicator的状态，但当前indicator的product有值了，说明是第一次有product的值，但不算发生了变化。
                // 因此，在indicatorProductMap中记录这个indicator的product
                indicatorCounter.put(indicatorId,1); // 代表这个indicator的product有值了，[经过确认，这个也算发生了变化]
                indicatorProductMap.put(indicatorId,products);

            }

        }else{
            // 还是0，则代表product有值，但没有发生变化，接下来变不变，要看当前indicator的值了
            assert indicatorProductMap.containsKey(indicatorId);

            if((justEquals(products,indicatorProductMap.get(indicatorId)))&&(!isConditionChanged(bean,bypassCondition))){
                // Product 没有发生变化
                // 之前存储的indicator的product和现在来的indicator的product一致，则计数器+1
                indicatorCounter.put(indicatorId,indicatorCounter.get(indicatorId)+1);
            }else{
                // 之前存储的indicator的product和现在来的indicator的product不一致，发生了改变，则：
                indicatorCounter.put(indicatorId,1);    // 将计数器置为1
                indicatorProductMap.put(indicatorId,products);  // 将product记录表中的product记录为当前indicator的product
            }


        }
    }


}
