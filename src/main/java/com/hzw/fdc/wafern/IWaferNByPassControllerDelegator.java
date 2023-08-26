package com.hzw.fdc.wafern;

import java.io.Serializable;

public interface IWaferNByPassControllerDelegator extends Serializable {

    public void open(Object[] args) throws Throwable;

    public void onUpdateConfig(Object[] args) throws Throwable;

    public boolean shouldByPass(Object[] args) throws Throwable;

}
