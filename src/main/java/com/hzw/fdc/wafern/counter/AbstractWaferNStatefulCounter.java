package com.hzw.fdc.wafern.counter;

import com.hzw.fdc.wafern.type.ByPassState;
import com.hzw.fdc.wafern.type.BypassCondition;
import com.hzw.fdc.wafern.type.IndicatorResultBean;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public abstract class AbstractWaferNStatefulCounter implements IWaferNByPassStatefulCounter, Serializable {

    protected Map<Long, Integer> indicatorCounter = new HashMap<>();
    protected Map<Long, Integer> byPassConditionValueMap = new HashMap<>();
    protected Map<Long, Integer> byPassConditionValueMapLast = new HashMap<>();


    protected Long indicatorCountKey(IndicatorResultBean bean){
        return  (bean.getToolName()+"|"+bean.getChamberName()+"|"+"|"+ bean.getIndicatorId()).hashCode()+0l;
    }

    @Override
    public void process(IndicatorResultBean bean, BypassCondition bypassCondition) throws Throwable {
        Long indicatorId = indicatorCountKey(bean);
        if(byPassConditionValueMap.containsKey(indicatorId)){
            byPassConditionValueMapLast.put(indicatorId,byPassConditionValueMap.get(indicatorId)); //记录上一次的配置值
        }
        byPassConditionValueMap.put(indicatorId,bypassCondition.getRecentRuns());
    }

    protected boolean isConditionChanged(IndicatorResultBean bean,BypassCondition bypassCondition){
        Long indicatorId = indicatorCountKey(bean);
        if(byPassConditionValueMapLast.containsKey(indicatorId)){
            return !byPassConditionValueMapLast.get(indicatorId).equals(bypassCondition.getRecentRuns());
        }else{
            return true;
        }
    }

    @Override
    public ByPassState getByPassState(IndicatorResultBean bean, BypassCondition bypassCondition) {
        //上面更新完indicator的状态后，就要根据bypassCondition里面具体的数值，看看是否有达到与之
        assert bypassCondition!=null;
        Long indicatorId = indicatorCountKey(bean);



        if(indicatorCounter.containsKey(indicatorId)){
            Integer count = indicatorCounter.get(indicatorId);
            if(count==0){
                return ByPassState.UnByPass;
            }
            if(count>bypassCondition.getRecentRuns()){
                return ByPassState.UnByPass;
            }else{
                return ByPassState.ByPass;
            }
        }else{
            return ByPassState.UNKNOWN;
        }
    }

    protected boolean justEquals(Object o1, Object o2){
        if((o1==null)&&(o2==null)) return true;
        if((o1==null)&&(o2!=null)) return false;
        if((o1!=null)&&(o2==null)) return false;
        return o1.equals(o2);
    }
}
