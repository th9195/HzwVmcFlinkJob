package com.hzw.fdc.wafern.counter.impls;

import com.hzw.fdc.wafern.counter.AbstractWaferNStatefulCounter;
import com.hzw.fdc.wafern.type.BypassCondition;
import com.hzw.fdc.wafern.type.IndicatorResultBean;

import java.io.Serializable;
import java.util.List;

public class FirstProductAfterPMCounter extends AbstractWaferNStatefulCounter implements Serializable {

    @Override
    public void process(IndicatorResultBean bean, BypassCondition bypassCondition) throws Throwable {

        Long indicatorId = indicatorCountKey(bean);
        List<String> products = bean.getProductName();
        if(bean.isPMStatusStartMatch()){
            if(indicatorCounter.containsKey(indicatorId)){
                indicatorCounter.remove(indicatorId);
            }
        }else if(bean.isPMStatusEndMatch()){
            if((products!=null)&&(!products.isEmpty())){  // 如果当前indicator 命中end，并且product不为空，则计数1
                indicatorCounter.put(indicatorId,1);
            }else{  // 如果当前indicator命中end，但product是空的，则计数0，表示命中了pm，但product没有命中，所以记0，等待后面出现product
                indicatorCounter.put(indicatorId,0);
            }
        }else{
            // 如果当前的indicator没有PM状态
            // 如果之前不存在这个indicator的PM状态，那就忽略。（因为，既没有以前的PM状态，当前的indicator页没有PM状态）
            if(!indicatorCounter.containsKey(indicatorId)){
                // ignore

            }else{

                Integer count = indicatorCounter.get(indicatorId);
                if(count == 0){ //说明仅仅是命中了pm，product还没有命中，

                    if((products!=null)&&(products.isEmpty())){
                        indicatorCounter.put(indicatorId,1);
                    }else{
                        // 忽略，因为虽然命中了pm，但至此时还没有命中product，所以不能计数。
                    }

                }else{ // 大于0，说明不仅命中了pm，还已经命中过product
                    indicatorCounter.put(indicatorId,count+1);
                }

            }
        }

    }

    @Override
    protected Long indicatorCountKey(IndicatorResultBean bean) {
        return (bean.getToolName()+"|"+bean.getChamberName()+"|"+ bean.getIndicatorId()+"|"+bean.getStandardizedProduct()).hashCode()+0l;
    }
}
