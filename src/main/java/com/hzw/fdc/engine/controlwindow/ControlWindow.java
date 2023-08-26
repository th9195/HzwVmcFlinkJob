package com.hzw.fdc.engine.controlwindow;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.hzw.fdc.engine.api.IControlWindow;
import com.hzw.fdc.engine.controlwindow.data.WindowIndicatorConfig;
import com.hzw.fdc.engine.controlwindow.data.defs.IDataFlow;
import com.hzw.fdc.engine.controlwindow.data.defs.IDataPacket;
import com.hzw.fdc.engine.controlwindow.data.defs.IIndicatorData;
import com.hzw.fdc.engine.controlwindow.data.defs.ISensorData;
import com.hzw.fdc.engine.controlwindow.data.impl.DataFlow;
import com.hzw.fdc.engine.controlwindow.data.impl.IndicatorData;
import com.hzw.fdc.engine.controlwindow.data.impl.WindowClipResponse;
import com.hzw.fdc.engine.controlwindow.data.impl.WindowClipResult;
import com.hzw.fdc.engine.controlwindow.exception.ChildrenControlWindowException;
import com.hzw.fdc.engine.controlwindow.exception.ControlWindowException;
import com.hzw.fdc.engine.controlwindow.exception.ControlWindowExpressionError;
import com.hzw.fdc.engine.controlwindow.exception.ParentControlWindowException;
import com.hzw.fdc.engine.controlwindow.expression.WindowExpressionParser;
import com.hzw.fdc.engine.controlwindow.logistic.SensorExpressionBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.*;

@Slf4j
public abstract class ControlWindow implements IControlWindow, Serializable {

    public static final String PRE_START = "_START";
    public static final String PRE_END = "_END";

    public static final String TYPE_TimeBasedWindow="TimeBasedWindow";
    public static final String TYPE_StepIdBasedWindow ="StepIdBasedWindow";
    public static final String TYPE_RSNStepIdBasedWindow ="RSNStepIdBasedWindow";
    public static final String TYPE_LogisticWindow ="LogisticWindow";
    public static final String TYPE_CycleWindowMax ="CycleWindowMax";
    public static final String TYPE_CycleWindowMin ="CycleWindowMin";
    private static final long serialVersionUID = -7424100704488943956L;


    protected boolean isRunning = false;

    protected boolean isAttaching = false;

    protected Long threadId = null;

    protected  ControlWindow parent = null;

    protected Long windowId;

    protected Map<String,Object> attributes = new HashMap<>();

    protected DataFlow dataFlow;

    protected DataFlow clippedDataFlowData;

    protected DataFlow orginalDataFlowPtr;

    protected List<WindowIndicatorConfig> windowIndicatorConfigList = new ArrayList<>();

    protected boolean canWindowEndBeDetermined = true;

    public boolean isCanWindowEndBeDetermined() {
//        System.out.println("ccc: "+canWindowEndBeDetermined);
        return canWindowEndBeDetermined;
    }

    public void setCanWindowEndBeDetermined(boolean canWindowEndBeDetermined) {
        this.canWindowEndBeDetermined = canWindowEndBeDetermined;
    }

    public abstract int getStartPosition() throws ControlWindowException;

    public abstract int getEndPosition() throws ControlWindowException;

    protected int  startPositionFound = -1;

    protected String startExp = null;
    protected String endExp = null;

    protected boolean fromBegin = false;

    protected boolean toEnd = false;

    protected boolean isDebugOn = false;

    protected String debugKey = "WindowDebugFlag";

    /**
     *  重置窗口的切分用状态，使得多次计算能否有个全新的state。
     */
    public void reset(){
        clippedDataFlowData = null;
        dataFlow = null;
        startPositionFound = -1;
    }

    public ControlWindow getParent(){
        return parent;
    }

    public boolean isFromBegin() {
        return fromBegin;
    }

    public void setFromBegin(boolean fromBegin) {
        this.fromBegin = fromBegin;
    }

    public boolean isToEnd() {
        return toEnd;
    }

    public void setToEnd(boolean toEnd) {
        this.toEnd = toEnd;
    }

    private IIndicatorData buildIndicatorData(WindowIndicatorConfig config, List<ISensorData> data){
        IndicatorData indicatorData = new IndicatorData();
        indicatorData.setData(data);
        indicatorData.setIndicatorId(config.indicatorId);
        indicatorData.setWindowId(windowId);
        indicatorData.setSensorAliasId(config.sensorAliasId);
        indicatorData.setSensorAliasName(config.sensorAliasName);
        return indicatorData;
    }

    public  List<IIndicatorData> calculateIndicatorData() throws ControlWindowException {
        List<IIndicatorData> indicatorDataList = new ArrayList<>();
        clippedDataFlowData = (DataFlow) getClippedDataFlow();
        for(WindowIndicatorConfig windowIndicatorConfig : windowIndicatorConfigList){
            List<ISensorData> data = clippedDataFlowData.getSensorDataList(windowIndicatorConfig.sensorAliasName);
            IIndicatorData indicatorData = buildIndicatorData(windowIndicatorConfig,data);
            indicatorData.setStartTime(clippedDataFlowData.getStartTime());
            indicatorData.setStopTime(clippedDataFlowData.getStopTime());
            indicatorDataList.add(indicatorData);
        }
        return indicatorDataList;
    }

    public synchronized Collection<WindowClipResult> calculate() throws ControlWindowException{
        Map<String,WindowClipResult> map = new HashMap<>();
        if(isRunning){
//            throw new ControlWindowException("wow=>[Window已经在计算中][不允许同一个window对象实例同时切割][windowId]:"+getWindowId()+"[ThreadId:"+Thread.currentThread().getId()+"]");
            log.warn("wow=>[Window已经在计算中][不允许同一个window对象实例同时切割][windowId]:"+getWindowId()+"[ThreadId:"+Thread.currentThread().getId()+"]");
        }else{
            threadId = Thread.currentThread().getId();
            isRunning = true;
        }
        try{
            List<IIndicatorData> list = calculateIndicatorData();
            Map<String,Integer> cycleUnitCountMap = new HashMap<>();
            String wcpKey = null;
            String cycleUnitCountKey = null;
            WindowClipResult wcp = null;
            for(IIndicatorData iIndicatorData: list){
                wcpKey = StringUtils.join(new Object[]{iIndicatorData.getWindowId(),iIndicatorData.getSensorAliasId(),iIndicatorData.getCycleUnitIndex()},"_");
                cycleUnitCountKey = StringUtils.join(new Object[]{iIndicatorData.getWindowId(),iIndicatorData.getIndicatorId(),iIndicatorData.getSensorAliasId()},"_");
                if(!map.containsKey(wcpKey)){
                    map.put(wcpKey,new WindowClipResult());
                }
                wcp = map.get(wcpKey);
                wcp.setWindowId(iIndicatorData.getWindowId());
                wcp.setSensorAliasId(iIndicatorData.getSensorAliasId());
                wcp.setCycleUnitIndex(iIndicatorData.getCycleUnitIndex());
                wcp.setData(iIndicatorData.getData());
                wcp.getIndicatorIds().add(iIndicatorData.getIndicatorId());

                try{
                    wcp.setStartTime(iIndicatorData.getStartTime());
                    wcp.setStopTime(iIndicatorData.getStopTime());
                }catch (Throwable t){
                    throw t;
                }
                try{
                    wcp.proceedBoundValue(this.orginalDataFlowPtr);
                }catch (Throwable t){
                    throw t;
                }
                if(!cycleUnitCountMap.containsKey(cycleUnitCountKey)){
                    cycleUnitCountMap.put(cycleUnitCountKey,1);
                }else{
                    cycleUnitCountMap.put(cycleUnitCountKey,cycleUnitCountMap.get(cycleUnitCountKey)+1);
                }
            }
            for(WindowClipResult windowClipResult: map.values()){
                windowClipResult.setCycleUnitCount(cycleUnitCountMap.get(windowClipResult.getWindowClipResultKey()));
            }
            return map.values();
        }catch (Throwable t){
            if(dataFlow==null){
                throw new ControlWindowException("wow=>[DataFlow为空异常]", t);
            }else{
                throw new ControlWindowException("wow=>[calculateException][dataFlow="+getAttachedDataFlow()+"][dataFlow.size="+getAttachedDataFlow().getDataPacketList().size()+"]",t);
            }

        }finally {
            isRunning = false;
            reset();
        }

    }

    public WindowClipResponse split() {
        try {
            List<WindowClipResponse.WindowSplitInfo> splitWindowInfo = splitWindow();
            return WindowClipResponse.builder()
                    .msgCode(WindowClipResponse.SUCCESS_CODE)
                    .msg("success")
                    .windowTimeRangeList(splitWindowInfo).build();
        } catch (Throwable t) {
            String msgCode = WindowClipResponse.COMMON_FAIL_CODE;
            if (t instanceof ParentControlWindowException) {
                msgCode = WindowClipResponse.PARENT_FAIL_CODE;
            } else if (t instanceof ChildrenControlWindowException) {
                msgCode = WindowClipResponse.CHILDREN_FAIL_CODE;
            }
            return WindowClipResponse.builder()
                    .msgCode(msgCode)
                    .msg(t.getMessage())
                    .windowTimeRangeList(Collections.emptyList())
                    .build();
        }
    }

    @Override
    public List<WindowClipResponse.WindowSplitInfo> splitWindow() throws ControlWindowException {
        IDataFlow dataFlow = getClippedDataFlow();
        boolean startInclude = false, endInclude = false;
        List<IDataPacket> dps = dataFlow.getDataList();
        if (dps.size() > 0) {
            startInclude = dps.get(0).getTimestamp() == dataFlow.getStartTime();
            endInclude = dps.get(dps.size() - 1).getTimestamp() == dataFlow.getStopTime();
        }
        return Lists.newArrayList(WindowClipResponse.WindowSplitInfo.builder()
                .windowId(getWindowId())
                .startTime(dataFlow.getStartTime())
                .startInclude(startInclude)
                .endTime(dataFlow.getStopTime())
                .endInclude(endInclude)
                .build());
    }

    public Long getWindowId() {
        return windowId;
    }

    public void setWindowId(Long windowId) {
        this.windowId = windowId;
    }

    public IDataFlow getAttachedDataFlow() {
        return dataFlow;
    }

    public void attachDataFlow(IDataFlow dataFlow) {
        try{
            if(isRunning){
//                throw new RuntimeException("wow=>[Window已经在计算中][不允许在切割过程中Attach另外的DataFlow][windowId]:"+getWindowId()+"[ThreadId:"+Thread.currentThread().getId()+"]");
                log.warn("wow=>[Window已经在计算中][不允许在切割过程中Attach另外的DataFlow][windowId]:"+getWindowId()+"[ThreadId:"+Thread.currentThread().getId()+"]");
            }else{
                if(isAttaching){
//                    throw new RuntimeException("wow=>[attachDataFlow进行中][不允许在attaching结束前重复attach][windowId]:"+getWindowId()+"[ThreadId:"+Thread.currentThread().getId()+"]");
                    log.warn("wow=>[attachDataFlow进行中][不允许在attaching结束前重复attach][windowId]:"+getWindowId()+"[ThreadId:"+Thread.currentThread().getId()+"]");
                }else{
                    isAttaching = true;
                }

                selfAttachDataFlow(dataFlow,true,true);
            }
        }catch (Throwable t){
            throw new RuntimeException("wow=>[attachDataFlow异常]:"+t.getMessage(),t);
        }finally {
            isAttaching = false;
        }
    }

    public void selfAttachDataFlow(IDataFlow dataFlow,boolean clone,boolean sort){

        reset();
        if(clone){
//            List<String> requiredSensorAlias =  getAllRequiredSensorAlias();
//            Set<String> requiredSensorAliasSet = new HashSet<>(requiredSensorAlias);
//            this.dataFlow = ((DataFlow) dataFlow).cloneSelf(requiredSensorAliasSet);
            // 不对dataFlow进行过滤
            this.dataFlow = (DataFlow) dataFlow;
        }
        if(sort){
            this.dataFlow.sort();
        }

        this.orginalDataFlowPtr = this.dataFlow;
    }

    public IDataFlow getClippedDataFlow() throws ControlWindowException {
        if(clippedDataFlowData ==null){
            int startPosition = getStartPosition();
            if((startPosition<0)||(startPosition>dataFlow.getDataList().size())) throw new ControlWindowException("START位置超出边界, position: "+startPosition+", range: [0,"+dataFlow.getDataList().size()+")");
            startPositionFound = startPosition;

            int endPosition = getEndPosition();
            if((endPosition<0)||(endPosition>dataFlow.getDataList().size())) throw new ControlWindowException("END位置超出边界, position: "+endPosition+", range: [0,"+dataFlow.getDataList().size()+")");

            int[] offsets = postOffset(startPosition,endPosition);
            if ((offsets[0] < 0) || (offsets[0] > dataFlow.getDataList().size())) throw new ControlWindowException("START位置超出边界，position: " + offsets[0]);
            if ((offsets[1] < 0) || (offsets[1] > dataFlow.getDataList().size())) throw new ControlWindowException("END位置超出边界，position: " + offsets[1]);
            if((offsets[0] >= offsets[1])){
                throw new ControlWindowException("START >= END "+"[windowId="+windowId+"][indicatorConfig:"+windowIndicatorConfigList+"][startPosition:"+offsets[0]+"][endPosition:"+offsets[1]+"]");
            }
            clippedDataFlowData = (DataFlow) dataFlow.getSubDataList(offsets[0],offsets[1]);
        }
        return clippedDataFlowData;
    }

    protected int[] postOffset(int startPosition, int endPosition) {
        return new int[]{startPosition,endPosition};
    }

    public List<WindowIndicatorConfig> getIndicatorConfigList() {
        return windowIndicatorConfigList;
    }

    public void setAttribute(String key, Object value){
        this.attributes.put(key,value);
    }

    public Object getAttribute(String key){
        return this.attributes.get(key);
    }

    @Override
    public String toString() {
        return toDefinitionJSON().toJSONString();
    }

    public String getStartExp() {
        return startExp;
    }

    public void setStartExp(String startExp) {
        this.startExp = startExp;
    }

    public String getEndExp() {
        return endExp;
    }

    public void setEndExp(String endExp) {
        this.endExp = endExp;
    }

    public void setParent(ControlWindow parent) {
        this.parent = parent;
    }

    public JSONObject toDefinitionJSON(){
        JSONObject object = new JSONObject();
        object.put("id",windowId);
        object.put("attrs",attributes);
        object.put("type",this.getClass().getSimpleName());
        JSONObject startObject = new JSONObject();
        JSONObject endObject = new JSONObject();
        object.put("start",startObject);
        object.put("end",endObject);
        startObject.put("exp",startExp);
        endObject.put("exp",endExp);
        object.put("indicators", windowIndicatorConfigList);
        if(this instanceof TimeBasedWindow){
            startObject.put("direction",((TimeBasedWindow) this).getDirection_start());
            startObject.put("value",((TimeBasedWindow) this).getValue_start());
            startObject.put("unit",((TimeBasedWindow) this).getUnit_start());

            endObject.put("direction",((TimeBasedWindow) this).getDirection_end());
            endObject.put("value",((TimeBasedWindow) this).getValue_end());
            endObject.put("unit",((TimeBasedWindow) this).getUnit_end());
        }
        if(this instanceof StepIdBasedWindow){
            startObject.put("stepId",((StepIdBasedWindow) this).getStepId_start());
            endObject.put("stepId",((StepIdBasedWindow) this).getStepId_end());
        }
        if(this instanceof RSNStepIdBasedWindow){
            startObject.put("rsn",((RSNStepIdBasedWindow) this).getRsn_start());
            endObject.put("rsn",((RSNStepIdBasedWindow) this).getRsn_end());
        }

        if((this instanceof ContainerWindow)&&(!((ContainerWindow) this).getSubWindows().isEmpty())){
            JSONArray array = new JSONArray();
            object.put("children",array);
            List<ControlWindow> subWindowList = ((ContainerWindow) this).getSubWindows();
            for(ControlWindow window: subWindowList){
                array.add(window.toDefinitionJSON());
            }
        }

        return object;
    }

    public abstract List<String> getRequiredSensorAlias();
    public abstract List<String> getAllRequiredSensorAlias();

    public List<String> getConfiguredIndicatorSensorAlias(){
        List<String> all = new ArrayList<>();
        List<WindowIndicatorConfig> windowIndicatorConfigs =  getIndicatorConfigList();
        for(WindowIndicatorConfig windowIndicatorConfig : windowIndicatorConfigs){
            all.add(windowIndicatorConfig.sensorAliasName);
        }
        return all;
    }

    public void addIndicatorConfig(WindowIndicatorConfig windowIndicatorConfig){
        if((windowIndicatorConfig.indicatorId==null)||(windowIndicatorConfig.sensorAliasId==null)||(windowIndicatorConfig.sensorAliasName==null)){
            throw new RuntimeException("wow=>[WindowIndicatorConfig-Error][Non-Null Field is null][indicatorId="+windowIndicatorConfig.indicatorId+"][sensorAliasId="+windowIndicatorConfig.sensorAliasId+"][sensorAliasName="+windowIndicatorConfig.sensorAliasName+"]");
        }

        {
            // 做重复WindowIndicatorConfig检查，最近发现有重复的windowIndicatorConfig发进来，这会导致错误的cycleCount的结果的，原来的设计是不应该出现这样的情况的。
            // 现在既然出现了，就堵死这条路，我先log判断出来是那里
            List<WindowIndicatorConfig> configList = getIndicatorConfigList();
            for(WindowIndicatorConfig indicatorConfig:configList){
                if(indicatorConfig.equals(windowIndicatorConfig)){
                    log.error("wow=>[警告,出现重复的WindowIndicatorConfig]:windowId={},windowStart={},windowEnd={},indicatorConfig={}",this.getWindowId(),getStartExp(),getEndExp(),indicatorConfig);
                    return;
                }
            }
        }


        getIndicatorConfigList().add(windowIndicatorConfig);
    }

    public static void addGlobalImport(String imp){
        SensorExpressionBuilder.addGlobalImport(imp);
    }

    public static ControlWindow parse(String type, String start, String end) throws ControlWindowExpressionError {
        return new WindowExpressionParser().parse(type,start,end);

    }


    @Override
    public void addIndicatorConfig(Long indicatorId, Long sensorAliasId, String sensorAliasName) {
        addIndicatorConfig(new WindowIndicatorConfig(indicatorId,sensorAliasId,sensorAliasName));
    }

    @Override
    public void addSubWindow(IControlWindow controlWindow) { }


    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }


    public DataFlow getDataFlow() {
        return dataFlow;
    }

    public void setDataFlow(DataFlow dataFlow) {
        this.dataFlow = dataFlow;
    }



    public List<WindowIndicatorConfig> getWindowIndicatorConfigList() {
        return windowIndicatorConfigList;
    }

    public void setWindowIndicatorConfigList(List<WindowIndicatorConfig> windowIndicatorConfigList) {
        this.windowIndicatorConfigList = windowIndicatorConfigList;
    }

    public int getStartPositionFound() {
        return startPositionFound;
    }

    public void setStartPositionFound(int startPositionFound) {
        this.startPositionFound = startPositionFound;
    }

    public DataFlow getClippedDataFlowData() {
        return clippedDataFlowData;
    }

    public void setClippedDataFlowData(DataFlow clippedDataFlowData) {
        this.clippedDataFlowData = clippedDataFlowData;
    }


    public String utilFormatOffset(char unit, char value){
        return "";
    }

    public String displayStartExp(){
        return "";
    }

    public String displayEndExp(){
        return "";
    }


    public boolean isDebugOn() {
        return isDebugOn;
    }

    public void setDebugOn(boolean debugOn) {
        isDebugOn = debugOn;
    }

    public String getDebugKey() {
        return debugKey;
    }

    public void setDebugKey(String debugKey) {
        this.debugKey = debugKey;
    }

    public String getSnapShotInfo(String debugKey){
        JSONObject snapShotInfo = new JSONObject();
        snapShotInfo.put("debugKey", debugKey);
        snapShotInfo.put("def", toDefinitionJSON());
        snapShotInfo.put("dataFlow", JSONObject.toJSONString(getAttachedDataFlow()));
        return snapShotInfo.toJSONString();
    }

    public JSONObject getSnapShotInfoAsJson(String debugKey){
        JSONObject snapShotInfo = new JSONObject();
        snapShotInfo.put("debugKey", debugKey);
        snapShotInfo.put("def", toDefinitionJSON());
        snapShotInfo.put("dataFlow", JSONObject.toJSONString(getAttachedDataFlow()));
        return snapShotInfo;
    }

}
