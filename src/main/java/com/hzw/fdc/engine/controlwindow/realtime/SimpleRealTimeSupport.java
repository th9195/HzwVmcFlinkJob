package com.hzw.fdc.engine.controlwindow.realtime;

import com.alibaba.fastjson.JSON;
import com.hzw.fdc.engine.Utils;
import com.hzw.fdc.engine.api.IControlWindow;
import com.hzw.fdc.engine.api.IRealTimeSupport;
import com.hzw.fdc.engine.api.IWindowClipListener;
import com.hzw.fdc.engine.controlwindow.ControlWindow;
import com.hzw.fdc.engine.controlwindow.data.defs.IDataFlow;
import com.hzw.fdc.engine.controlwindow.data.defs.IDataPacket;
import com.hzw.fdc.engine.controlwindow.data.impl.DataFlow;
import com.hzw.fdc.engine.controlwindow.data.impl.WindowClipResult;
import com.hzw.fdc.engine.controlwindow.exception.RealTimeWindowClipException;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

public class SimpleRealTimeSupport<T extends IControlWindow>  implements IRealTimeSupport, Serializable {

    public final static String CLOSE_REASON_DUPLICATE_OPEN = "Window重复开启异常";



    static Logger log = LoggerFactory.getLogger(SimpleRealTimeSupport.class);

    private static final long serialVersionUID = -6059773325306286233L;
    private IWindowClipListener windowClipListener;

    protected T delegateControlWindow;

    protected Set<WindowClipResult> clippedResult = new HashSet<>();

    protected IDataFlow dataFlow = null;

    private boolean isOpened = false;

    private String openedSessionId = null;

    private boolean isFinalTryCalled = false;

    private String closeReason = "初始化状态[从未执行open]";

    private int clipCount = 0;

    private Collection<WindowClipResult> tmpResult = null;

    public  boolean canTryClip(){
        return true;
    }

    public synchronized List<WindowClipResult> tryClip() throws RealTimeWindowClipException {
        if(isFinalTryCalled){
            String msg = "wow=>[ERROR][RealTimeWindow][tryClip]: 该Window的finalTry()方法已经被调用，不允许再继续调用tryClip()。";
            throw new RealTimeWindowClipException(msg);
        }
        if(!isOpened){
            String msg = "wow=>[ERROR][RealTimeWindow][tryClip]: Window为关闭状态，不允许进行tryClip操作, window为关闭态的原因："+this.closeReason;
            log.info("{}",msg);
            this.isOpened = false;
            this.openedSessionId = null;
            this.isFinalTryCalled = false;
            throw new RealTimeWindowClipException(msg);
        }
        try{

            int size = dataFlow.getDataList().size();
            if(size>=clipCount){

                Collection<WindowClipResult> results;
                if((tmpResult == null)||(tmpResult.isEmpty())){
                    //
                    delegateControlWindow.attachDataFlow(dataFlow);
                    results = delegateControlWindow.calculate();
                    if((results!=null)&&(!results.isEmpty())){
                        tmpResult = results;
                        /**
                         *  在最后一个点时切割命中时其实已经有results了，缺返回了null，因此修复下面的return null；语句。禁用掉。
                         */
//                        return null;
                    }
                }else{
                    delegateControlWindow.attachDataFlow(dataFlow);
                    results = delegateControlWindow.calculate();
                    tmpResult = null;
                }




                if((results==null)||(results.isEmpty())) {
                    tmpResult = null;
                    return null;
                }else{
                    tmpResult = results;
                }


                /**
                 * WindowEnd情况下，在未达到结尾时，dataFlow是未设置stopTime的；所以，如果切出来的窗口没有stopTime，
                 * 说明窗口的stopTime就是当前dataFlow最后一个点的时间
                 */
                long stopTime = dataFlow.getDataList().get(dataFlow.getDataList().size()-1).getTimestamp();
                results.forEach(wcp -> {
                    if (wcp.getStopTime() == null) {
                        wcp.setStopTime(stopTime);
                    }
                });

                List<WindowClipResult> pResult = new ArrayList<>();
                for (WindowClipResult windowClipResult: results){
                    if(!clippedResult.contains(windowClipResult)){
                        clippedResult.add(windowClipResult);
                        try{
                            if(windowClipListener!=null){
                                windowClipListener.onWindowClipped(windowClipResult,delegateControlWindow);
                            }
                            pResult.add(windowClipResult);

                        }catch (Throwable t){
                            throw new RealTimeWindowClipException("wow=>[error][control window]: "+delegateControlWindow.toString()+"\t"+"datasize:"+delegateControlWindow.getAttachedDataFlow().toString()+"\t"+t.getMessage(),t);
                        }
                    }else{
                        clippedResult.add(windowClipResult);
                    }
                }

                if(pResult.isEmpty()) {
//                System.out.println("count: "+dataFlow.getDataList().size()+" pResult:"+results);
                    return null;
                }
                return pResult;
            }else{
                return null;
            }



        }catch (Throwable t){
            StringBuilder sb = new StringBuilder();
            if(delegateControlWindow instanceof ControlWindow ){
                String windowDef = ((ControlWindow) delegateControlWindow).toDefinitionJSON().toJSONString();
                String dataFlow = JSON.toJSONString(this.dataFlow);
//                System.out.println(t.getMessage());
                log.info("wow=>[ERROR][RealTimeWindow][tryClip][unknown]: {},\nwindowDef:{}\ndataFlow:{}", Utils.throwable2String(t),windowDef,dataFlow);
                return null;
            }
//            System.out.println(t.getMessage());
//            log.info("wow=>[ERROR][RealTimeWindow][tryClip][unknown]: {},", Utils.throwable2String(t));
            return null;
        }finally {
            clipCount++;
        }
    }

    @Override
    public List<WindowClipResult> finalTryClip() throws RealTimeWindowClipException {
        if(!isOpened){
            String msg = "wow=>[ERROR][RealTimeWindow][finalTryClip]: Window为关闭状态，不允许进行finalTryClip操作, window为关闭态的原因："+this.closeReason;
            log.info("{}",msg);
            this.isOpened = false;
            this.openedSessionId = null;
            this.isFinalTryCalled = false;
            throw new RuntimeException(msg);
        }

        if(delegateControlWindow instanceof ControlWindow){
            ((ControlWindow) delegateControlWindow).setCanWindowEndBeDetermined(true);
        }
        List<WindowClipResult> l = tryClip();
        this.isFinalTryCalled = true;
        return l;
    }

    public SimpleRealTimeSupport(T delegateControlWindow) {
        this(delegateControlWindow,null);
    }

    public SimpleRealTimeSupport(T delegateControlWindow, IWindowClipListener windowClipListener) {
        this.windowClipListener = windowClipListener;
        this.delegateControlWindow = delegateControlWindow;
        if(this.delegateControlWindow instanceof ControlWindow){
            ((ControlWindow) this.delegateControlWindow).setCanWindowEndBeDetermined(false);
        }
    }

    public void setDelegateControlWindow(T delegateControlWindow) {
        log.info("wow=>[WARN][RealTimeWindow][setDelegateControlWindow]:该实时window的窗口配置发生了变化，不建议这样做，检查逻辑，请构造一个新的SimpleRealTimeSupport对象。");
        this.delegateControlWindow = delegateControlWindow;
    }

    @Override
    public void setWindowClipListener(IWindowClipListener listener) {
        this.windowClipListener = listener;

    }

    @Override
    public synchronized T open() {

        // 检查， 看看这个window的状态是否是已经开启的状态。
        //
        if(isOpened){
            // warn. 这个window已经是open状态了，不允许在未关闭的时候再次open。
            String errorSuggestMsg = "这个window已经是open状态了，不允许在未关闭的时候再次open。请检查real time window的api调用逻辑，确保在调用open方法前这个window没有没打开过或已经是关闭状态";
            log.info("wow=>[WARN][RealTimeWindow][open]: {}",errorSuggestMsg);
            //为了安全起见，将状态进行重置
            this.isOpened = false;
            this.openedSessionId = null;
            this.isFinalTryCalled = false;
            this.closeReason = CLOSE_REASON_DUPLICATE_OPEN;
            throw new RuntimeException(errorSuggestMsg);
        }
        dataFlow = DataFlow.build();
        if(delegateControlWindow instanceof ControlWindow){
            ((ControlWindow) delegateControlWindow).setCanWindowEndBeDetermined(false);
        }
        if(windowClipListener!=null){
            try{
                windowClipListener.onOpened(delegateControlWindow);
            }catch (Throwable t){
                t.printStackTrace();
            }
        }


        this.isOpened = true;
        this.openedSessionId = UUID.randomUUID().toString();
        this.closeReason="";

        return delegateControlWindow;
    }

    @Override
    public synchronized void addAll(List<IDataPacket> dataPacket) {
        if(!isOpened){
            String msg = "wow=>[ERROR][RealTimeWindow][add]: Window为关闭状态，不允许进行add操作, window为关闭态的原因："+this.closeReason;
            log.info("{}",msg);

            this.isOpened = false;
            this.openedSessionId = null;
            this.isFinalTryCalled = false;
            throw new RuntimeException(msg);
        }
        if(dataFlow!=null){
            dataFlow.getDataList().addAll(dataPacket);
        }else{
            throw new RuntimeException("实时window的dataFlow对象为null");
        }

    }

    /**
     *  清空之前的sensorList数据
     */
    @Override
    public synchronized void clear(){
        if(dataFlow!=null) {
            dataFlow.getDataList().clear();
        }
    }


    @Override
    public synchronized Set<WindowClipResult> close() {
        if(!isOpened){
            log.info("wow=>[WARN][RealTimeWindow][close]: 发现调用close()方法时，该window并未开启，虽然该操作并未禁止，但不推荐，因为可能存在调用逻辑问题，请检查。");
        }
        if(!isFinalTryCalled){
            log.info("wow=>[WARN][RealTimeWindow][close]: 发现调用close()方法时，finalTryClip()方法并未调用，虽然该操作并未禁止，但不推荐，因为这样可能导致一部分window（结尾边界）无法正确切分，请检查。");
        }
        if(windowClipListener!=null){
            try{
                windowClipListener.onClosed(delegateControlWindow, clippedResult);
            }catch (Throwable t){
                t.printStackTrace();
            }
        }
        clippedResult = new HashSet<>();
        ((ControlWindow)delegateControlWindow).reset();
        if(delegateControlWindow instanceof ControlWindow){
            ((ControlWindow) delegateControlWindow).setCanWindowEndBeDetermined(false);
        }

        isOpened = false;
        this.openedSessionId = null;
        this.isFinalTryCalled = false;
        this.closeReason = "Close主动被调用使得Window被关闭";
        return clippedResult;
    }

    @Override
    public IControlWindow getDelegateWindow() {
        return delegateControlWindow;
    }

    @Override
    public String toString() {
        ControlWindow controlWindow = (ControlWindow) delegateControlWindow;
        return "SimpleRealTimeSupport: "+delegateControlWindow.toString();
    }

    public IWindowClipListener getWindowClipListener() {
        return windowClipListener;
    }

    public T getDelegateControlWindow() {
        return delegateControlWindow;
    }

    public Set<WindowClipResult> getClippedResult() {
        return clippedResult;
    }

    public void setClippedResult(Set<WindowClipResult> clippedResult) {
        this.clippedResult = clippedResult;
    }

    public IDataFlow getDataFlow() {
        return dataFlow;
    }

    @Override
    public void setStartTime(Long startTime) {
        if(dataFlow!=null){
            dataFlow.setStartTime(startTime);
        }
    }

    @Override
    public void setStopTime(Long stopTime) {
        if(dataFlow!=null){
            dataFlow.setStopTime(stopTime);
        }
    }

    public void setDataFlow(IDataFlow dataFlow) {
        this.dataFlow = dataFlow;
    }





}
