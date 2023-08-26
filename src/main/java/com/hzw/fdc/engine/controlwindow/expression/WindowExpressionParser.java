package com.hzw.fdc.engine.controlwindow.expression;

import com.hzw.fdc.engine.controlwindow.*;
import com.hzw.fdc.engine.controlwindow.exception.ControlWindowExpressionError;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.hzw.fdc.engine.controlwindow.ControlWindow.*;
import static com.hzw.fdc.engine.controlwindow.expression.WindowExpressionValidator.*;

public class WindowExpressionParser {


    public String determineControlWindowType(String start, String end){
        if(PRE_START.equals(start)&&PRE_END.equals(end)){
            return TYPE_TimeBasedWindow;
        }
        if(validateTimeBasedControlWindow(start,end)){
            return TYPE_TimeBasedWindow;
        }
        if(validateStepBasedControlWindow(start,end)){
            return TYPE_StepIdBasedWindow;
        }
        if(validateRSNStepBasedControlWindow(start,end)){
            return TYPE_RSNStepIdBasedWindow;
        }
        if(validateLogisticControlWindow(start,end)){
            return TYPE_LogisticWindow;
        }
        if(validateCycleControlWindowMax(start,end)){
            return TYPE_CycleWindowMax;
        }
        if(validateCycleControlWindowMin(start,end)){
            return TYPE_CycleWindowMin;
        }



        return null;

    }

    /**
     * 通过start和end表达式生成一个ControlWindow。
     * 注意，该版本是在对ControlWindow增加controlWindowType属性之前的，所以为了适配它，实现了一个determineControlWindowType方法
     * 来自动的判断controlWindow的类型，但能力是有限的，后续，还是请使用带type参数的的parse。
     * @param start
     * @param end
     * @return
     * @throws ControlWindowExpressionError
     */
    public ControlWindow parse( String start, String end) throws ControlWindowExpressionError{
        String type = determineControlWindowType(start,end);
        if(type==null) throw new ControlWindowExpressionError.UnDefinedExpressionError();
        try{
            start = start.replace("max|","");
        }catch (Throwable t){ }
        try{
            start = start.replace("min|","");
        }catch (Throwable t){ }
        return parse(type,start,end);
    }

    public ControlWindow parse(String type, String start, String end) throws ControlWindowExpressionError {
        start = start.trim();
        end = end.trim();

        if (ControlWindow.TYPE_TimeBasedWindow.equals(type)) {
            return parseTimeBasedWindow(start, end);
        } else if (ControlWindow.TYPE_StepIdBasedWindow.equals(type)) {
            return parseStepBasedWindow(start, end);
        } else if (ControlWindow.TYPE_RSNStepIdBasedWindow.equals(type)) {
            return parseRSNStepBasedWindow(start,end);
        } else if (ControlWindow.TYPE_LogisticWindow.equals(type)) {
            start = start.replace("{", "").replace("}", "").trim();
            end = end.replace("{", "").replace("}", "").trim();
            return parseLogisticWindow(start, end);
        } else if(TYPE_CycleWindowMax.equals(type)||TYPE_CycleWindowMin.equals(type)){
            start = start.trim();
            String[] parts = end.split("_");
            String pattern = null;
            if(end.startsWith("_END_")){
                end = "_"+parts[1];
                pattern = parts[2];
            }else{
                end = parts[0];
                pattern = parts[1];
            }
            boolean isMaxMode = TYPE_CycleWindowMax.equals(type);
            return parseCycleWindow(start,end,pattern,isMaxMode);
        }
        else {
            throw new ControlWindowExpressionError.UnDefinedExpressionError();
        }
    }

    private ControlWindow parseCycleWindow(String start,  String end, String pattern,boolean isMaxMode) throws ControlWindowExpressionError.CycleControlWindowExpressionError {
        try {
            CycleWindow cycleWindow = new CycleWindow(pattern,isMaxMode,start,end);
            return cycleWindow;
        } catch (Exception e) {
            throw new ControlWindowExpressionError.CycleControlWindowExpressionError("CycleWindow表达式错误:start="+", end="+end+", pattern="+pattern+", isMaxMode="+isMaxMode);
        }
    }

    private ControlWindow parseLogisticWindow(String start, String end) throws ControlWindowExpressionError.LogisticControlWindowExpressionError {
        LogisticWindow window = new LogisticWindow();
        window.setExp(start, end);
        return window;
    }

    private ControlWindow parseRSNStepBasedWindow(String start, String end) throws ControlWindowExpressionError {
        try{
            Pattern pattern = Pattern.compile("(C)(\\d+)(S)([-]{0,1}\\d+)([be])(\\d+)([psS])");
            RSNStepIdBasedWindow window = new RSNStepIdBasedWindow();

            if(PRE_START.equals(start)){
                window.setFromBegin(true);
            }else{
                window.setFromBegin(false);
                Matcher matcher = pattern.matcher(start);
                if (matcher.find()) {

                    window.setRsn_start(Integer.parseInt(matcher.group(2)));
                    window.setStepId_start(Integer.parseInt(matcher.group(4)));
                    window.setDirection_start(matcher.group(5).charAt(0));
                    window.setValue_start(Integer.parseInt(matcher.group(6)));
                    window.setUnit_start(matcher.group(7).charAt(0));

                }else{
                    throwException("RSNStepBasedControlWindow表达式错误",start,end,null);
                }

            }

            if(ControlWindow.PRE_END.equals(end)){
                window.setToEnd(true);
            }else{
                window.setToEnd(false);
                Matcher matcher = pattern.matcher(end);
                if (matcher.find()) {

                    window.setRsn_end(Integer.parseInt(matcher.group(2)));
                    window.setStepId_end(Integer.parseInt(matcher.group(4)));
                    window.setDirection_end(matcher.group(5).charAt(0));
                    window.setValue_end(Integer.parseInt(matcher.group(6)));
                    window.setUnit_end(matcher.group(7).charAt(0));

                }else{
                    throwException("RSNStepBasedControlWindow表达式错误",start,end,null);
                }

            }

            return window;
        }catch (Throwable t){
            throwException("RSNStepBasedControlWindow表达式错误",start,end,t);
        }
        return null;
    }

    private ControlWindow parseStepBasedWindow(String start, String end) throws ControlWindowExpressionError {
        try {
            Pattern pattern = Pattern.compile("(S)([-]{0,1}\\d+)([be])(\\d+)([psS])");
            StepIdBasedWindow window = new StepIdBasedWindow();

            if (PRE_START.equals(start)) {
                window.setFromBegin(true);
            } else {
                window.setFromBegin(false);
                Matcher matcher = pattern.matcher(start);
                if (matcher.find()) {

                    window.setStepId_start(Integer.parseInt(matcher.group(2)));
                    window.setDirection_start(matcher.group(3).charAt(0));
                    window.setValue_start(Integer.parseInt(matcher.group(4)));
                    window.setUnit_start(matcher.group(5).charAt(0));

                } else {
                    throwException("StepBasedControlWindow表达式错误", start, end, null);
                }
            }

            if (ControlWindow.PRE_END.equals(end)) {
                window.setToEnd(true);
            } else {
                window.setToEnd(false);
                Matcher matcher = pattern.matcher(end);
                if (matcher.find()) {
                    window.setStepId_end(Integer.parseInt(matcher.group(2)));
                    window.setDirection_end(matcher.group(3).charAt(0));
                    window.setValue_end(Integer.parseInt(matcher.group(4)));
                    window.setUnit_end(matcher.group(5).charAt(0));
                } else {
                    throwException("StepBasedControlWindow表达式错误", start, end, null);
                }
            }
            return window;
        } catch (Throwable t) {
            throw new ControlWindowExpressionError("StepBasedControlWindow表达式错误:start=" + start + ",end=" + end, t);
        }
    }

    public static void printMatcher(Matcher matcher) {
        int count = matcher.groupCount();
//        System.out.println("group count: " + count);
        for (int i = 0; i <= count; i++) {
            System.out.println(i + ":" + matcher.group(i));
        }
    }

    private ControlWindow parseTimeBasedWindow(String start, String end) throws ControlWindowExpressionError {
        try {
            Pattern pattern = Pattern.compile("(T)([be])(\\d+)([psS]){1}");
            TimeBasedWindow window = new TimeBasedWindow();
            if (PRE_START.equals(start)) {
                window.setFromBegin(true);
            } else {
                window.setFromBegin(false);
                Matcher matcher = pattern.matcher(start);
                if (matcher.find()) {

                    window.setDirection_start(matcher.group(2).charAt(0));
                    window.setValue_start(Integer.parseInt(matcher.group(3)));
                    window.setUnit_start(matcher.group(4).charAt(0));

                } else {
                    throwException("TimeBasedWindow表达式错误", start, end, null);
                }
            }
            if (ControlWindow.PRE_END.equals(end)) {
                window.setToEnd(true);
            } else {
                window.setToEnd(false);
                Matcher matcher = pattern.matcher(end);
                if (matcher.find()) {

                    window.setDirection_end(matcher.group(2).charAt(0));
                    window.setValue_end(Integer.parseInt(matcher.group(3)));
                    window.setUnit_end(matcher.group(4).charAt(0));

                } else {
                    throwException("TimeBasedWindow表达式错误", start, end, null);
                }
            }
            return window;
        } catch (Throwable t) {
            throw new ControlWindowExpressionError("TimeBasedWindow表达式错误:start=" + start + ",end=" + end, t);
        }
    }


    private void throwException(String msgPrefix, String start, String end, Throwable t) throws ControlWindowExpressionError {

        throw new ControlWindowExpressionError(msgPrefix + ":start=" + start + ",end=" + end, t);
    }


    public static void main(String[] args) throws ControlWindowExpressionError {
        WindowExpressionParser parser = new WindowExpressionParser();
//        ControlWindow window = parser.parse("Tb5p","Te1p");
//        ControlWindow window = parser.parse("Tb5p", "END");
        ControlWindow window = parser.parse("TimeBasedWindow", "S2b2p", "S3e2s");
//        ControlWindow window = parser.parse("LogisticWindow","{A>1}", "_END");
        System.out.println(window);


    }
}
