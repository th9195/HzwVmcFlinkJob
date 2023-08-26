package com.hzw.fdc.engine.api;

import com.hzw.fdc.engine.controlwindow.data.defs.IDataFlow;
import com.hzw.fdc.engine.controlwindow.data.defs.IDataPacket;
import com.hzw.fdc.engine.controlwindow.data.impl.WindowClipResult;
import com.hzw.fdc.engine.controlwindow.exception.RealTimeWindowClipException;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Set;

public interface IRealTimeSupport {

    public void setWindowClipListener(IWindowClipListener listener);

    public IControlWindow open();

    public void addAll(List<IDataPacket> dataPacket);

    public void clear();

    public List<WindowClipResult> tryClip() throws RealTimeWindowClipException;

    public List<WindowClipResult> finalTryClip() throws RealTimeWindowClipException;

    public Set<WindowClipResult> close();

    public IControlWindow getDelegateWindow();

    public IDataFlow getDataFlow();

    public void setStartTime(Long startTime);

    public void setStopTime(Long stopTime);


}
