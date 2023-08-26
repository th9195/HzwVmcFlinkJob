package com.hzw.fdc.engine.controlwindow.cycle;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class CycleUnitUtil {

    public static final String STOPWOARD = "@";
    public final static String rangePatternString = "(\\[[ ]*"+"([-]?\\b\\d+\\b)"+"[ ]*\\-[ ]*"+"([-]?\\b\\d+\\b)"+"[ ]*\\])";
    public final static Pattern rangePattern = Pattern.compile(rangePatternString);

    public static String preparePattern(String p, boolean max) throws Throwable{
        p = p.replace("(","").replace(")","");
        String[] st = p.split(",");
        List<String> l = new ArrayList<>();
        for(String s:st){
            s = s.trim();
            if(s.isEmpty()) {continue;}
            if("?".equals(s)){
                l.add(singleNumberPattern()) ;
            }else if("*".equals(s)){
                l.add(anyNumbers(max));
            }else if(s.startsWith("[")&&s.endsWith("]")){
                Integer[] p1  = extractRange(s);
                if(p1==null){
                    throw new Exception("Rnage Pattern error: "+s);
                }else{
                    l.add(rangePattern(p1[0],p1[1]));
                }
            }else{
                l.add(STOPWOARD+s+STOPWOARD);
            }
        }
        StringBuilder sb = new StringBuilder();
        sb.append("(");
        for(int i=0;i<l.size();i++){
            if(l.get(i).contains("*")){
                sb.append(l.get(i));
            }else
            {
                sb.append(l.get(i)).append(",");
            }
        }
        if(sb.charAt(sb.length()-1)==','){
            sb.deleteCharAt(sb.length()-1);
        }
        sb.append(")");
        return sb.toString();

    }

    private static String singleNumberPattern(){
        return "@?-?\\d+@?,?";
    }

    private static String anyNumbers(boolean max){
        if(max){
            return "(@?-?\\d+@?,?)*";
        }else {
            return "(@?-?\\d+@?,?)*?";
        }

    }

    private static String rangePattern(int start, int end){
        StringBuilder sb = new StringBuilder();
        for(int i=start;i<=end;i++){
            if(i!=end){
                sb.append("(@"+i+"@,?)|");
            }else{
                sb.append("(@"+i+"@,?)");
            }

        }
        return "("+sb.toString()+")";
    }

    public static List<CycleUnitPtr> extractCycleUnit(String patternReg, String msg, boolean max) throws Throwable{
        return CycleUnitUtilV1.extractCycleUnit(patternReg, msg, max);
    }
    public static List<CycleUnitPtr> extractCycleUnit_OldImpl(String patternReg, String msg, boolean max) throws Throwable {

        {
            String[] pt = msg.split(",");
            StringBuilder sb = new StringBuilder();
            for(String s:pt){
                sb.append("@").append(s).append("@").append(",");
            }
            if(sb.length()>0){
                sb.deleteCharAt(sb.length()-1);
            }
            msg = sb.toString();
        }

        List<CycleUnitPtr> cycleUnitPtrList = new ArrayList<>();
        String toCompile = preparePattern(patternReg,max);
        Pattern pattern = Pattern.compile(toCompile);
        Matcher matcher = pattern.matcher(msg);
        while (matcher.find()){
            if(matcher.group(0).isEmpty()) continue;

            String mstart = msg.substring(0, matcher.start(0));
            String mend = msg.substring(0,matcher.end(0));
            int indexStart = 0;
            if(mstart.contains(",")){
                indexStart = mstart.split(",").length;
            }
            int indexEnd = mend.split(",").length;
            String group0 = matcher.group(0);
            CycleUnitPtr ptr = new CycleUnitPtr();
            ptr.startIndex = indexStart;
            ptr.endIndex = indexEnd;
            ptr.cycleUnitStr = group0;

            ptr.steps = Arrays.stream(ptr.cycleUnitStr.replaceAll("@","").split(",")).filter(s->(s!=null)&&(!s.isEmpty())).map(s->Integer.parseInt(s)).collect(Collectors.toList());
            cycleUnitPtrList.add(ptr);
        }

        return cycleUnitPtrList;
    }

    public static Integer[] extractRange(String msg){
        Matcher matcher = rangePattern.matcher(msg);
        Integer[] found = null;
        while (matcher.find()){
            int start = Integer.parseInt(matcher.group(2));
            int end = Integer.parseInt(matcher.group(3));
            found = new Integer[]{start,end};
            return found;
        }
        return null;
    }




}
