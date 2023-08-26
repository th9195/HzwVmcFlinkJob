package com.hzw.fdc.engine;

import java.io.PrintWriter;
import java.io.StringWriter;

public class Utils {
    public static String string(Object... parts){
        StringBuilder sb = new StringBuilder();
        for(Object o: parts){
            sb.append(o);
        }
        return sb.toString();
    }

    public static String join(String split,Object... parts){
        StringBuilder sb = new StringBuilder();
        int last = parts.length-1;
        for(int i=0;i<parts.length;i++){
            if(i==last){
                sb.append(parts[i]);
            }else{
                sb.append(parts[i]).append(split);
            }
        }
        return sb.toString();
    }


    public static String throwable2String(Throwable t){
        try {
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            t.printStackTrace(pw);
            sw.close();
            pw.close();
            return sw.toString();
        } catch (Throwable e2) {
            return "wow=>[util-throwable2String-error]:"+e2;
        }
    }
}
