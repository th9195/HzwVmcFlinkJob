package com.hzw.fdc.wafern;

import com.hzw.fdc.wafern.type.IndicatorResultBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

public abstract class AbstractWaferNByPassDelegator implements IWaferNByPassControllerDelegator, Serializable {

    static Logger logger = LoggerFactory.getLogger(AbstractWaferNByPassDelegator.class);

    @Override
    public void open(Object[] args) throws Throwable {

    }

    @Override
    public void onUpdateConfig(Object[] args) throws Throwable {

    }


    private boolean isFlinkIndicatorResult(Object obj){
        return false;
    }

    @Override
    public boolean shouldByPass(Object[] args) throws Throwable {
        if((args==null)||(args.length<=0)){
            throw new Exception("Does not accept empty args.");
        }else{
            if(args[0] instanceof IndicatorResultBean){
               return _shouldByPassImpl((IndicatorResultBean)args[0]);
            }else{
                logger.error("wow=>[AbstractWaferNProcesssor][This must not be reached]");
                return false;
            }
        }
    }

    protected abstract boolean _shouldByPassImpl(IndicatorResultBean indicatorResult) throws Throwable;


}
