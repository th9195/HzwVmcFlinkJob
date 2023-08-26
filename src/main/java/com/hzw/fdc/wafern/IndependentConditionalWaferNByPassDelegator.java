package com.hzw.fdc.wafern;

import com.hzw.fdc.wafern.counter.impls.FirstProductAfterPMCounter;
import com.hzw.fdc.wafern.counter.impls.FirstRunAfterPMCounter;
import com.hzw.fdc.wafern.counter.impls.ProductChangedCounter;
import com.hzw.fdc.wafern.counter.impls.RecipeChangedCounter;
import com.hzw.fdc.wafern.type.ByPassState;
import com.hzw.fdc.wafern.type.BypassCondition;
import com.hzw.fdc.wafern.type.IndicatorResultBean;
import com.hzw.fdc.wafern.counter.*;
import com.hzw.fdc.wafern.type.WaferNConditionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IndependentConditionalWaferNByPassDelegator extends AbstractWaferNByPassDelegator implements Serializable {

    Logger logger = LoggerFactory.getLogger(IndependentConditionalWaferNByPassDelegator.class);

    Map<WaferNConditionType, IWaferNByPassStatefulCounter> waferNStateCounters = new HashMap<>();

    public IndependentConditionalWaferNByPassDelegator() {
        init();
    }

    private void init(){
        waferNStateCounters.put(WaferNConditionType.FirstProductAfterPM,new FirstProductAfterPMCounter());
        waferNStateCounters.put(WaferNConditionType.FirstRunAfterPM,new FirstRunAfterPMCounter());
        waferNStateCounters.put(WaferNConditionType.RecipeChanged,new RecipeChangedCounter());
        waferNStateCounters.put(WaferNConditionType.ProductChanged,new ProductChangedCounter());
    }

    @Override
    protected boolean _shouldByPassImpl(IndicatorResultBean indicatorResult) throws Throwable {
        List<BypassCondition> bypassConditionList = indicatorResult.getBypassCondition();
        if(bypassConditionList==null){
            return false;
        }

        //保证得到一个每个类型只有一条记录的map，如果得不到就是异常配置。委托下面的方法去搞定并记录日志
        Map<WaferNConditionType,BypassCondition> bypassConditionMap = assertUniquenessOfEachTypeByPassCondition(bypassConditionList,indicatorResult);

        Map<WaferNConditionType, ByPassState> indicatorWaferNCounterState = new HashMap<>();

        for(WaferNConditionType type: bypassConditionMap.keySet()){
            if(waferNStateCounters.containsKey(type)){
                waferNStateCounters.get(type).process(indicatorResult,bypassConditionMap.get(type));
                ByPassState state = waferNStateCounters.get(type).getByPassState(indicatorResult, bypassConditionMap.get(type));
                indicatorWaferNCounterState.put(type,state);
            }
        }

        //

        boolean judgementResult =  isByPass(indicatorWaferNCounterState, indicatorResult);

        return judgementResult;
    }

    /**
     *   |------------TYPE----------|-----VALUE-----|
     *   |----First Run After PM----|----ByPass-----|
     *   |--First Product After PM--|----UnByPass---|
     *   |------ProductChanged------|----UnByPass---|
     *   |------RecipeChanged-------|----UNKNOWN----|
     *
     * @param indicatorWaferNCounterState
     * @param indicatorResult
     * @return
     */
    private boolean isByPass(Map<WaferNConditionType, ByPassState> indicatorWaferNCounterState, IndicatorResultBean indicatorResult) {

        for(ByPassState state: indicatorWaferNCounterState.values()){
            if(state.equals(ByPassState.ByPass)){
                return true;
            }
        }
        return false;
    }


    /**
     * 确保每一个类型的bypass只有一条配置记录，如果同一个类型的bypasss有多条，则抛出异常，并记录日志
     * @param bypassConditionList
     */
    private Map<WaferNConditionType,BypassCondition> assertUniquenessOfEachTypeByPassCondition(List<BypassCondition> bypassConditionList,IndicatorResultBean indicatorResult) throws Throwable {
        Map<WaferNConditionType,BypassCondition> map = new HashMap<>();
        for(BypassCondition bypassCondition: bypassConditionList){
            if(map.containsKey(bypassCondition.getCondition())){
                logger.error("WaferN ByPassCondition Design Violation: Configuration Item Must Unique Per Condition. IndicatorId={} Condition={}",indicatorResult.getIndicatorId(),bypassCondition.getCondition());

                throw new Exception("WaferN ByPassCondition Design Violation: Configuration Item Must Unique Per Condition. IndicatorId="+indicatorResult.getIndicatorId()+", Condition: "+bypassCondition.getCondition());
            }else{
                map.put( WaferNConditionType.parse(bypassCondition.getCondition()),bypassCondition);
            }
        }

        return map;
    }
}
