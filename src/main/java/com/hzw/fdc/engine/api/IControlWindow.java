package com.hzw.fdc.engine.api;

import com.hzw.fdc.engine.controlwindow.data.WindowIndicatorConfig;
import com.hzw.fdc.engine.controlwindow.data.defs.IDataFlow;
import com.hzw.fdc.engine.controlwindow.data.impl.WindowClipResponse;
import com.hzw.fdc.engine.controlwindow.data.impl.WindowClipResult;
import com.hzw.fdc.engine.controlwindow.exception.ControlWindowException;
import com.hzw.fdc.engine.controlwindow.exception.ControlWindowExpressionError;
import com.hzw.fdc.engine.controlwindow.expression.WindowExpressionParser;
import scala.Serializable;

import java.util.Collection;
import java.util.List;

public interface IControlWindow extends Serializable {

    public void setWindowId(Long windowId);

    WindowClipResponse split();

    List<WindowClipResponse.WindowSplitInfo> splitWindow() throws ControlWindowException;

    public Long getWindowId();

    public void addIndicatorConfig(Long indicatorId, Long sensorAliasId, String sensorAliasName);

    public List<WindowIndicatorConfig> getIndicatorConfigList();

    public void addSubWindow(IControlWindow controlWindow);

    public void attachDataFlow(IDataFlow dataFlow);
    public IDataFlow getAttachedDataFlow();

    public Collection<WindowClipResult> calculate() throws ControlWindowException;

    /**
     * 暴露一个接口，这个方法能够范围这个window切分时所需要的所有sensor列表。
     * @return
     */
    public List<String> getRequiredSensorAlias();

    /**
     * 暴露一个接口，这个方法能够范围这个window切分时所需要的所有sensor列表,包括自己的子窗口。
     * @return
     */
    public List<String> getAllRequiredSensorAlias();


    public static IControlWindow parse(String type, String start, String end) throws ControlWindowExpressionError {
        return new WindowExpressionParser().parse(type,start,end);

    }
}
