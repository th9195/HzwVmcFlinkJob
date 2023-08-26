package com.hzw.fdc.engine.controlwindow.logistic;

import com.hzw.fdc.engine.controlwindow.AbstractExpressionScript;
import com.hzw.fdc.engine.controlwindow.ControlWindow;
import com.hzw.fdc.engine.controlwindow.data.defs.IDataPacket;
import com.hzw.fdc.engine.controlwindow.data.impl.Range;

public interface IExpressionScript {
    public boolean eval(IDataPacket dataPacket, AbstractExpressionScript.DataPackIndex index, ControlWindow controlWindow, Range range) throws Throwable;
}
