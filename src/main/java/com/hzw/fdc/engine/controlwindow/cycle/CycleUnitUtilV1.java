package com.hzw.fdc.engine.controlwindow.cycle;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class CycleUnitUtilV1 {

    public static final String STOPWOARD = "";
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
                l.add(","+s+",");
            }
        }
        StringBuilder sb = new StringBuilder();
        sb.append("(");
        for(int i=0;i<l.size();i++){
            sb.append(l.get(i));
        }
//        if(sb.charAt(sb.length()-1)==','){
//            sb.deleteCharAt(sb.length()-1);
//        }
        sb.append(")");
        String patt = sb.toString();
        return sb.toString();

    }

    private static String singleNumberPattern(){
        return ",\\d+,";
    }

    private static String anyNumbers(boolean max){
        if(max){
            return "(,\\d+,){0,500}";
        }else {
            return "(,\\d+,){0,500}?";
        }

    }

    private static String rangePattern(int start, int end){
        int min = Math.min(start, end);
        int max = Math.max(start, end);
        List<String> strList = new ArrayList<>();
        for(int i=min;i<=max;i++){
            strList.add("(," + i + ",)");
        }
        return StringUtils.join("(", StringUtils.join(strList, "|"), ")");
    }

    public static String buildMatchableMsg(String[] pt,int length){
        {

            StringBuilder sb = new StringBuilder();
            for(int i=0;i<length;i++){
                String s = pt[i];
                sb.append("@").append(s).append("@").append(",");
            }
            if(sb.length()>0){
                sb.deleteCharAt(sb.length()-1);
            }
            String msg = sb.toString();
            return msg;
        }

    }

    public static List<CycleUnitPtr> extractCycleUnit(String patternReg, List<Integer> stepIds, boolean max) throws Throwable{
        StringBuilder sb = new StringBuilder();
        for(Integer i: stepIds){
            sb.append(i).append(",");
        }
        return extractCycleUnit(patternReg, sb.toString(), max);
    }

    public static List<CycleUnitPtr> extractCycleUnit(String patternReg, String stepIds, boolean max) throws Throwable {

        if(!stepIds.endsWith(",")){
            stepIds = stepIds+",";
        }
        if(!stepIds.startsWith(",")){
            stepIds = ","+stepIds;
        }
//       stepIds = stepIds.replace(",", ",,");
        String checkStepIdStrs = stepIds.replace(",", ",,");

        List<CycleUnitPtr> cycleUnitPtrList = new ArrayList<>();

        String toCompile = preparePattern(patternReg,max);
        Pattern pattern = Pattern.compile(toCompile);
        Matcher matcher = pattern.matcher(checkStepIdStrs);
        while (matcher.find()){
            if(matcher.group(0).isEmpty()) continue;
            String group0 = matcher.group(0);
            String mstart = checkStepIdStrs.substring(0, matcher.start(0));
            String mend = checkStepIdStrs.substring(0,matcher.end(0));
            mstart = mstart.replace(",,",",");
            mend = mend.replace(",,",",");
            int indexStart = ",".equals(mstart) ? 0 : mstart.split(",").length-1;
            int indexEnd = ",".equals(mend) ? 0 : mend.split(",").length-1;

            CycleUnitPtr ptr = new CycleUnitPtr();
            ptr.startIndex = indexStart;
            ptr.endIndex = indexEnd;
            ptr.cycleUnitStr = group0;

            ptr.steps = Arrays.stream(ptr.cycleUnitStr.replaceAll(",,",",").split(",")).filter(s->(s!=null)&&(!s.isEmpty())).map(s->Integer.parseInt(s)).collect(Collectors.toList());
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
