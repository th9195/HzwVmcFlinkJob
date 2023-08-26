package com.hzw.fdc.engine.api;

import com.hzw.fdc.engine.controlwindow.ControlWindow;
import com.hzw.fdc.engine.controlwindow.data.impl.WindowClipResult;

import java.util.Set;

public interface IWindowClipListener {
    public void onOpened(IControlWindow controlWindow);
    public void onWindowClipped(WindowClipResult windowClipResult, IControlWindow controlWindow);
    public void onClosed(IControlWindow controlWindow, Set<WindowClipResult> allWindowClipResults);
}
