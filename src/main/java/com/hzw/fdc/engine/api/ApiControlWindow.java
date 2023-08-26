package com.hzw.fdc.engine.api;

import com.alibaba.fastjson.JSONObject;
import com.hzw.fdc.engine.controlwindow.AbstractExpressionScript;
import com.hzw.fdc.engine.controlwindow.ControlWindow;
import com.hzw.fdc.engine.controlwindow.data.defs.IDataFlow;
import com.hzw.fdc.engine.controlwindow.data.defs.IDataPacket;
import com.hzw.fdc.engine.controlwindow.data.defs.ISensorData;
import com.hzw.fdc.engine.controlwindow.data.impl.*;
import com.hzw.fdc.engine.controlwindow.exception.ControlWindowException;
import com.hzw.fdc.engine.controlwindow.exception.ControlWindowExpressionError;
import com.hzw.fdc.engine.controlwindow.exception.ExceptionUtils;
import com.hzw.fdc.engine.controlwindow.expression.WindowExpressionParser;
import com.hzw.fdc.engine.controlwindow.logistic.IExpressionScript;
import com.hzw.fdc.engine.controlwindow.logistic.SensorExpressionBuilder;
import com.hzw.fdc.engine.controlwindow.realtime.SimpleRealTimeSupport;
import com.hzw.fdc.scalabean.ClipWindowResult;
import com.hzw.fdc.scalabean.ClipWindowTimestampInfo;
import com.hzw.fdc.scalabean.WindowData;
import com.hzw.fdc.scalabean.sensorDataList;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.mutable.MutableList;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ApiControlWindow {

    static Logger logger = LoggerFactory.getLogger(ApiControlWindow.class);

    public static IControlWindow parse(String type, String start, String end) throws ControlWindowExpressionError {
        return new WindowExpressionParser().parse(type,start,end);
    }

    /**
     *  打开或关闭window的调试状态，打开后，在调用ApiControlWindow.calculate(IControlWindow window)时，会在log中输出调试信息。
     * @param window
     * @param isOn
     */
    public synchronized static void setDebugStateForWindow(IControlWindow window, boolean isOn, String debugKey){
        ControlWindow cw = (ControlWindow) window;
        cw.setDebugKey(debugKey);
        cw.setDebugOn(isOn);
    }

    /**
     * 获取指定窗口当前的信息快照，返回的是一个字符串。里面包含window数的配置信息和当前配置的dataflow信息。
     * @param controlWindow
     * @return
     */
    public synchronized static String getWindowSnapshotInfo(IControlWindow controlWindow, String debugKey){
        try{
            ControlWindow cw = (ControlWindow) controlWindow;
            return cw.getSnapShotInfo(cw.getDebugKey());
        }catch (Throwable t){
            return new ExceptionUtils().stackTrace(t);
        }
    }

    public synchronized static String getWindowSnapshotInfo(IRealTimeSupport controlWindow, String debugKey){
        try{
            ControlWindow cw = (ControlWindow) controlWindow.getDelegateWindow();
            JSONObject obj = cw.getSnapShotInfoAsJson(cw.getDebugKey());
            obj.put("dataFlow",controlWindow.getDataFlow());
            return obj.toJSONString();
        }catch (Throwable t){
            return new ExceptionUtils().stackTrace(t);
        }
    }

    public synchronized static  MutableList<WindowData> calculate(IControlWindow window) throws ControlWindowException{

        Collection<WindowClipResult> windowClipResults = null;
        MutableList<WindowData> list = new MutableList<>();
        try {
            windowClipResults = window.calculate();
            if(window instanceof ControlWindow){
                if(((ControlWindow) window).isDebugOn()){
                    logger.error("wow=>[WindowSnapshot][{}][result]:{},{}",((ControlWindow) window).getDebugKey(),
                            ((ControlWindow) window).getSnapShotInfo(((ControlWindow) window).getDebugKey()),
                            windowClipResults
                    );
                }
            }
        }catch (Throwable t){
            if(window instanceof ControlWindow){
                if(((ControlWindow) window).isDebugOn()){
                    logger.error("wow=>[WindowSnapshot][{}][result][failed]:{},{}",((ControlWindow) window).getDebugKey(),
                            ((ControlWindow) window).getSnapShotInfo(((ControlWindow) window).getDebugKey()),
                            windowClipResults
                    );
                }
            }
//            throw new ControlWindowException(t.getMessage(),t);
            return list;
        }

        for(WindowClipResult wcp: windowClipResults){

            try {
                list.appendElem(convertWindowClipResultToWindowData(wcp));
            } catch (Exception e) {

                logger.info("wow=>[sensorDataList为empty而导致的异常]");
                // 这里可以看看如何处理 sensorDataList为empty而导致的异常，尽管可能不会存在，但需要坐下记录，目前是catch住并跳过了，
                // 建议这里 添加一个记日志的功能
                // 该异常由toScalaBean()方法抛出
//                throw new ControlWindowException("wow=>[sensorDataList为empty而导致的异常]",e);
            }
        }
        return list;
    }

    public synchronized static MutableList<WindowData> calculate(IControlWindow window, boolean canWindowEndBeDetermined) throws ControlWindowException {
        ((ControlWindow) window).setCanWindowEndBeDetermined(canWindowEndBeDetermined);
        return calculate(window);
    }

    public static WindowData convertWindowClipResultToWindowData(WindowClipResult wcp) throws Exception{
        String sensorName;
        String sensorAlias;
        String unit;
        MutableList<sensorDataList> list = new MutableList();
        MutableList<Object> indicatorIds = new MutableList<>();

        {
            for(ISensorData sensorData: wcp.getData()){
                list.appendElem(new sensorDataList(sensorData.getValue(),sensorData.getTimestamp(),sensorData.getStepId()));
            }
        }

        {
            try{
                if(wcp.getData().isEmpty()){ // 增加调试信息
                    logger.error("wow=>[error][clip-result-isEmpty][windowId:{}][indicatorIds:{}]",wcp.getWindowId(),wcp.getIndicatorIds());
                }
            }catch (Throwable t){}

            if(wcp.getData().isEmpty()){
                throw new Exception("sensor data list is empty, can not get first node to get sensor name and sensor alias. sensorId="+wcp.getSensorAliasId());
            }else{
                sensorName = wcp.getData().get(0).getSensorName();
                sensorAlias = wcp.getData().get(0).getSensorAlias();
                unit = wcp.getData().get(0).getUnit();
            }
        }

        {
            for(Long l:wcp.getIndicatorIds()){
                indicatorIds.appendElem(l);
            }
        }

        return new WindowData(wcp.getWindowId(), sensorName,sensorAlias,unit,indicatorIds.toList(),wcp.getCycleUnitCount().intValue(),wcp.getCycleUnitIndex().intValue(),wcp.getStartTime(),wcp.getStopTime(), Option.apply(wcp.getStartTimeValue()),Option.apply(wcp.getStopTimeValue()),list.toList());
    }

    public ClipWindowResult split(IControlWindow window, boolean windowEndBeDetermined) {
        ((ControlWindow) window).setCanWindowEndBeDetermined(windowEndBeDetermined);
        WindowClipResponse response = window.split();
        MutableList<ClipWindowTimestampInfo> windowTimeRangeList = new MutableList<>();
        if (response.getWindowTimeRangeList() != null) {
            response.getWindowTimeRangeList().forEach(windowTimeRange -> {
                windowTimeRangeList.appendElem(new ClipWindowTimestampInfo(windowTimeRange.getWindowId(), windowTimeRange.getStartTime(), windowTimeRange.getStartInclude(), windowTimeRange.getEndTime(), windowTimeRange.getEndInclude()));
            });
        }

        return new ClipWindowResult(response.getMsgCode(), response.getMsg(), windowTimeRangeList.toList());
    }

    public static IRealTimeSupport addRealTimeSupport(IControlWindow controlWindow){
        return new SimpleRealTimeSupport(controlWindow);
    }

    public static IDataFlow buildDataFlow(){
        return DataFlow.build();
    }

    public static IDataPacket buildDataPacket(){
        return DataPacket.build();
    }

    public static ISensorData buildSensorData(){
        return SensorData.build();
    }

    public static List<String> utilParseSensors(String logicExpression) throws Throwable {
        String safeExpression = preProcessExpression(logicExpression);
        AbstractExpressionScript script = (AbstractExpressionScript) new SensorExpressionBuilder(safeExpression).build();
        return script.getVariables();
    }

    public static String preProcessExpression(String logicExp){
        for(int i=0;i<5;i++){
            if(logicExp.startsWith("((((((")&&logicExp.contains("))))))")) {
                logicExp = logicExp.replace("(((((", "").replace(")))))", "");
            }else{
                return logicExp;
            }
        }
        return logicExp;
    }

    public static void main(String[] args) {
        try {
            //(CMPSEN002>22&&StepID==4)
            List<String> sensors = ApiControlWindow.utilParseSensors("(((((((((((CMPSEN002>22&&StepID==4)))))))))))&&offset()");
            System.out.println(sensors);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        String t= "(((((((((((CMPSEN002>22&&StepID==4)))))))))))&&offset()";
        String p = preProcessExpression(t);
        System.out.println(p);
    }


}
