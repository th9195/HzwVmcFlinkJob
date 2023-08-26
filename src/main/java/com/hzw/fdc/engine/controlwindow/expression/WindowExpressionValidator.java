package com.hzw.fdc.engine.controlwindow.expression;

import com.github.javaparser.ast.expr.Expression;
import com.hzw.fdc.engine.controlwindow.ControlWindow;
import com.hzw.fdc.engine.controlwindow.CycleWindow;
import com.hzw.fdc.engine.controlwindow.logistic.IExpressionScript;
import com.hzw.fdc.engine.controlwindow.logistic.SensorExpressionBuilder;

import static com.hzw.fdc.engine.controlwindow.ControlWindow.*;

public class WindowExpressionValidator {

    public static boolean validateTimeBasedControlWindow(String start, String end){
        String v = start+"$"+end;
        return v.matches("T[be]{1}\\d+[psS]{1}\\$T[be]{1}\\d+[psS]{1}")
                ||v.matches(PRE_START+"\\$T[be]{1}\\d+[psS]{1}")
                ||v.matches("T[be]{1}\\d+[psS]{1}\\$"+PRE_END);
    }

    public static boolean validateStepBasedControlWindow(String start, String end){
        String v= start+"$"+end;
        return v.matches("S[-]{0,1}\\d+[be]{1}\\d+[psS]{1}\\$S[-]{0,1}\\d+[be]{1}\\d+[psS]{1}")
                ||v.matches(PRE_START+"\\$S[-]{0,1}\\d+[be]{1}\\d+[psS]{1}")
                ||v.matches("S[-]{0,1}\\d+[be]{1}\\d+[psS]{1}\\$"+PRE_END);
    }

    public static boolean validateRSNStepBasedControlWindow(String start, String end){
        String v= start+"$"+end;
        return v.matches("C\\d+S[-]{0,1}\\d+[be]{1}\\d+[psS]{1}\\$C\\d+S[-]{0,1}\\d+[be]{1}\\d+[psS]{1}")
                ||v.matches(PRE_START+"\\$C\\d+S[-]{0,1}\\d+[be]{1}\\d+[psS]{1}")
                ||v.matches("C\\d+S[-]{0,1}\\d+[be]{1}\\d+[psS]{1}\\$"+PRE_END);
    }

    public static boolean validateCycleControlWindowMax(String start, String end){
        IExpressionScript expr = null;
        try {
            String type = "";
            if(start.startsWith("max|")){
                type= TYPE_CycleWindowMax;
            }else{
                return false;
            }
            parse(type,start,end);
        }catch (Throwable t){
            t.printStackTrace();
            return false;
        }
        return true;
    }

    public static boolean validateCycleControlWindowMin(String start, String end){
        IExpressionScript expr = null;
        try {
            String type = "";
            if(start.startsWith("min|")){
                type= TYPE_CycleWindowMin;
            }else{
                return false;
            }
            parse(type,start,end);
        }catch (Throwable t){
            t.printStackTrace();
            return false;
        }
        return true;
    }

    public static boolean validateLogisticControlWindow(String start, String end){
        IExpressionScript expr = null;
        try {
            if(!PRE_START.equals(start)){
                expr = new SensorExpressionBuilder(start).build();
            }

            if(!PRE_END.equals(end)){
                expr = new SensorExpressionBuilder(end).build();
            }

        }catch (Throwable t){
            t.printStackTrace();
            return false;
        }
        return true;
    }

    public static boolean isBinary(Expression expression) throws Exception {
        if(expression.isEnclosedExpr()){
            Expression inner = expression.asEnclosedExpr().getInner();
            return isBinary(inner);
        }else if(expression.isBinaryExpr()){
            System.out.println(expression);
            return true;
        }else{
            throw new Exception(expression.toString());
        }
    }

    public static boolean validateControlWindowExpression(String type,String start, String end){
        if(ControlWindow.TYPE_TimeBasedWindow.equals(type)){
            return validateTimeBasedControlWindow(start,end);
        }else if(ControlWindow.TYPE_StepIdBasedWindow.equals(type)){
            return validateStepBasedControlWindow(start,end);
        } else if(ControlWindow.TYPE_RSNStepIdBasedWindow.equals(type)){
            return validateRSNStepBasedControlWindow(start,end);
        }else if(ControlWindow.TYPE_LogisticWindow.equals(type)){
            start = start.replace("{","").replace("}","").trim();
            end = end.replace("{","").replace("}","").trim();
            return validateLogisticControlWindow(start,end);
        }else{
            return false;
        }
    }

    public static void main(String[] args) {
//        System.out.println(validateControlWindowExpression("LogisticWindow","{(temp1>60)||((temp1+temp2)>110)}","_END"));   // true
//        System.out.println(validateControlWindowExpression("LogisticWindow","_START","{(temp1>60)||((temp1+temp2)>110)}")); // true
//        System.out.println(validateControlWindowExpression("LogisticWindow","_START","_END")); //true
//        System.out.println(validateControlWindowExpression("LogisticWindow","{(temp1>60)||((temp1+temp2)>110<<<)}","END")); //false
        System.out.println(validateControlWindowExpression("TimeBasedWindow","Tb0p","_END")); //false





    }



}
