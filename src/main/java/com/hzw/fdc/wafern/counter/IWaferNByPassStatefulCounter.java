package com.hzw.fdc.wafern.counter;

import com.hzw.fdc.wafern.type.ByPassState;
import com.hzw.fdc.wafern.type.BypassCondition;
import com.hzw.fdc.wafern.type.IndicatorResultBean;

import java.io.Serializable;

public interface IWaferNByPassStatefulCounter extends Serializable {

    void process(IndicatorResultBean bean, BypassCondition bypassCondition) throws Throwable;

    ByPassState getByPassState(IndicatorResultBean bean, BypassCondition bypassCondition);

}
