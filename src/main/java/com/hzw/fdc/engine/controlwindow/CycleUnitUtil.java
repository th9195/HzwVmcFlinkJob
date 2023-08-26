package com.hzw.fdc.engine.controlwindow;

import com.hzw.fdc.engine.controlwindow.exception.ControlWindowExpressionError;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class CycleUnitUtil {

    public static String preparePattern(String p, boolean max) throws Throwable{
        p = p.trim();
        if(!(p.startsWith("(")&&p.endsWith(")"))){
            throw new ControlWindowExpressionError.CycleControlWindowExpressionError("未以\"(\"开头或未以\")\"结尾");
        }
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
            }else if(s.matches("[-]?\\b\\d+\\b[,]?")){
                l.add(s);
            }else{
                throw new ControlWindowExpressionError.CycleControlWindowExpressionError("非法表达式字符");
            }
        }

        StringBuilder sb = new StringBuilder();
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
        return "("+sb.toString()+")";

    }

    private static String singleNumberPattern(){
        return "[-]?\\b\\d+\\b[,]?";
    }

    private static String anyNumbers(boolean max){
        if(max){
            return "([-]?\\b\\d+\\b[,]?)*";
        }else {
            return "([-]?\\b\\d+\\b[,]?)*?";
        }

    }

    private static String rangePattern(int start, int end){
        StringBuilder sb = new StringBuilder();
        for(int i=start;i<=end;i++){
            if(i!=end){
                sb.append("("+i+"[,]?)|");
            }else{
                sb.append("("+i+"[,]?)");
            }

        }
        return "("+sb.toString()+")";
    }

    public static List<CycleUnitPtr> extractCycleUnit(String patternReg, String msg, boolean max) throws Throwable {

        List<CycleUnitPtr> cycleUnitPtrList = new ArrayList<>();
        String toCompile = preparePattern(patternReg,max);
        Pattern pattern = Pattern.compile(toCompile);

        Matcher matcher = pattern.matcher(msg);
        while (matcher.find()){

//            System.out.println("i="+0+" => "+matcher.group(0)+" => start = "+matcher.start(0)+" end = "+matcher.end(0));
            if(matcher.group(0).isEmpty()) continue;
            String mstart = msg.substring(0, matcher.start(0));
            String mend = msg.substring(0,matcher.end(0));
            int indexStart = mstart.split(",").length;
            int indexEnd = mend.split(",").length;
            if(indexStart==1){
                indexStart = 0;
            }

            CycleUnitPtr ptr = new CycleUnitPtr();
            ptr.startIndex = indexStart;
            ptr.endIndex = indexEnd;
            ptr.cycleUnitStr = matcher.group(0);
//            System.out.println("\t"+mstart+"\t->"+indexStart);
//            System.out.println("\t"+mend+"\t->"+indexEnd);
            ptr.steps = Arrays.stream(ptr.cycleUnitStr.split(",")).filter(s->(s!=null)&&(!s.isEmpty())).map(s->Integer.parseInt(s)).collect(Collectors.toList());
            cycleUnitPtrList.add(ptr);
//            System.out.println("\t"+mstart.co+"\t"+mend);
        }

//        System.out.println(cycleUnitPtrList);
        return cycleUnitPtrList;
    }

    public static Integer[] extractRange(String msg){
        Pattern rangePattern = Pattern.compile("(\\[[ ]*"+"([-]?\\b\\d+\\b)"+"[ ]*\\-[ ]*"+"([-]?\\b\\d+\\b)"+"[ ]*\\])");
        Matcher matcher = rangePattern.matcher(msg);
        Integer[] found = null;
        while (matcher.find()){
//            for(int i=0;i<=matcher.groupCount();i++){
//                System.out.println(i+"-> "+matcher.group(i)+" -> ");
//            }
            int start = Integer.parseInt(matcher.group(2));
            int end = Integer.parseInt(matcher.group(3));
            found = new Integer[]{start,end};
            return found;
        }
        return null;
    }




}
