package com.hzw.fdc.engine.controlwindow;

import com.hzw.fdc.engine.api.IControlWindow;
import com.hzw.fdc.engine.controlwindow.data.defs.IIndicatorData;
import com.hzw.fdc.engine.controlwindow.data.impl.WindowClipResponse;
import com.hzw.fdc.engine.controlwindow.exception.ChildrenControlWindowException;
import com.hzw.fdc.engine.controlwindow.exception.ControlWindowException;
import com.hzw.fdc.engine.controlwindow.exception.ParentControlWindowException;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class ContainerWindow extends ControlWindow implements Serializable {



    protected List<ControlWindow> subWindows = new ArrayList<>();

    @Override
    public List<IIndicatorData> calculateIndicatorData() throws ControlWindowException {
        List<IIndicatorData> all = new ArrayList<>();
        List<IIndicatorData> selfIndicatorData = super.calculateIndicatorData();
        all.addAll(selfIndicatorData);
        for(ControlWindow subWindow: subWindows){
            subWindow.attachDataFlow(getClippedDataFlow());
            List<IIndicatorData> subWindowIndicatorData = subWindow.calculateIndicatorData();
            all.addAll(subWindowIndicatorData);
        }
        return all;
    }

    @Override
    public List<WindowClipResponse.WindowSplitInfo> splitWindow() throws ControlWindowException {
        List<WindowClipResponse.WindowSplitInfo> all = new ArrayList<>();
        try {
            List<WindowClipResponse.WindowSplitInfo> selfWindowSplitInfos = super.splitWindow();
            all.addAll(selfWindowSplitInfos);
        } catch (Exception ex) {
            if (subWindows != null && subWindows.size() > 0) {
                throw new ParentControlWindowException(ex.getMessage());
            }
            throw ex;
        }
        for (ControlWindow subWindow : subWindows) {
            subWindow.attachDataFlow(getClippedDataFlow());
            try {
                List<WindowClipResponse.WindowSplitInfo> subWindowSplitInfos = subWindow.splitWindow();
                all.addAll(subWindowSplitInfos);
            } catch (Exception ex) {
                throw new ChildrenControlWindowException(ex.getMessage());
            }
        }
        return all;
    }

    public List<ControlWindow> getSubWindows() {
        return subWindows;
    }

    public void addSubWindow(ControlWindow controlWindow){
        getSubWindows().add(controlWindow);
        controlWindow.setParent(this);
    }

    @Override
    public void addSubWindow(IControlWindow controlWindow) {
        if(controlWindow instanceof ControlWindow){
            addSubWindow((ControlWindow)controlWindow);
        }
    }

    @Override
    public List<String> getRequiredSensorAlias() {
        Set<String> all = new HashSet<>();
        if(this instanceof LogisticWindow){
            all.addAll(((LogisticWindow) this).getRequiredSensors());
        }
        all.addAll(getConfiguredIndicatorSensorAlias());

        return all.stream().collect(Collectors.toList());
    }

    @Override
    public List<String> getAllRequiredSensorAlias() {
        Set<String> all = new HashSet<>();
        if(this instanceof LogisticWindow){
            all.addAll(((LogisticWindow) this).getRequiredSensors());
        }
        all.addAll(getConfiguredIndicatorSensorAlias());
        for(ControlWindow subWindow: subWindows){
            all.addAll(subWindow.getRequiredSensorAlias());
        }
        return all.stream().collect(Collectors.toList());
    }


    public void setSubWindows(List<ControlWindow> subWindows) {
        this.subWindows = subWindows;
    }
}
