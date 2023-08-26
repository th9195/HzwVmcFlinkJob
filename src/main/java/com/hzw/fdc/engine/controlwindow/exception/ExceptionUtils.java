package com.hzw.fdc.engine.controlwindow.exception;

import java.io.PrintWriter;
import java.io.StringWriter;

public class ExceptionUtils {

    /**
     * 将stack trace信息转化为字符串
     * @param t
     * @return
     */
    public String stackTrace(Throwable t){
        try {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            t.printStackTrace(pw);
            sw.close();
            pw.close();
            return sw.toString();
        } catch (Exception e2) {
            return "wow=>[stackTraceUtilsError]["+e2.getMessage()+"]:"+t.toString();
        }
    }
}
