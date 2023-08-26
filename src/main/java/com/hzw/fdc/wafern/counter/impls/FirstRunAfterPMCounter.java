package com.hzw.fdc.wafern.counter.impls;

import com.hzw.fdc.wafern.counter.AbstractWaferNStatefulCounter;
import com.hzw.fdc.wafern.type.BypassCondition;
import com.hzw.fdc.wafern.type.IndicatorResultBean;

import java.io.Serializable;

public class FirstRunAfterPMCounter extends AbstractWaferNStatefulCounter implements Serializable {

    @Override
    public void process(IndicatorResultBean bean, BypassCondition bypassCondition) throws Throwable {
//        Long indicatorId = bean.getIndicatorId();
        Long indicatorId = indicatorCountKey(bean);
        if(bean.isPMStatusStartMatch()){
            if(indicatorCounter.containsKey(indicatorId)){
                indicatorCounter.remove(indicatorId);
            }
        }else if(bean.isPMStatusEndMatch()){
            // 如果当前的indicator有PM状态
            // 则，不论之前是否有状态，将这个indicator的状态重置，设置成1;
            indicatorCounter.put(indicatorId,1);

        }else{
            // 如果当前的indicator没有PM状态
            // 如果之前不存在这个indicator的PM状态，那就忽略。（因为，既没有以前的PM状态，当前的indicator页没有PM状态）
            if(!indicatorCounter.containsKey(indicatorId)){
                // ignore

            }else{
                // 如果之前以及存在这个indicator的PM的状态，且还没有清除，那么将计数器+1
                indicatorCounter.put(indicatorId,indicatorCounter.get(indicatorId)+1);
            }
        }
    }



}
