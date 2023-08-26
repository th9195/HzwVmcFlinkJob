package com.hzw.fdc.engine.controlwindow.logistic;

import bsh.Interpreter;
import com.github.javaparser.StaticJavaParser;
import com.github.javaparser.ast.expr.BinaryExpr;
import com.github.javaparser.ast.expr.Expression;
import com.github.javaparser.ast.expr.NameExpr;
import com.github.javaparser.ast.expr.UnaryExpr;
import com.hzw.fdc.engine.controlwindow.AbstractExpressionScript;
import com.hzw.fdc.engine.controlwindow.exception.ExpressionError;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.function.Consumer;

public class SensorExpressionBuilder {

    List<String> variables = new ArrayList<>();

    private List<String> imports = new ArrayList<>();

    static List<String> globalImports = new ArrayList<>();

    String expression = "";

    public String getExpression() {
        return expression;
    }

    public void setExpression(String expression) {
        this.expression = expression;
    }

    public SensorExpressionBuilder() {
    }

    public SensorExpressionBuilder(String expression) {
        this.expression = preProcessExpression(expression);
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

    public void addImport(String imp){
        imports.add(imp);
    }

    public static void addGlobalImport(String imp){
        globalImports.add(imp);
    }

    public BinaryExpr unWrap(Expression expression) throws Exception {
        if(expression.isEnclosedExpr()){
            return unWrap(expression.asEnclosedExpr().getInner());
        }else if(expression.isBinaryExpr()){
            return expression.asBinaryExpr();
        }else if(expression.isUnaryExpr()){
            UnaryExpr ue = expression.asUnaryExpr();
            if(ue.getOperator().equals(UnaryExpr.Operator.LOGICAL_COMPLEMENT)){
                return unWrap(ue.getExpression());
            }else{
                throw new Exception("not support unary expression: "+expression.toString());
            }
        }
        else{
            throw new Exception("not a valid binary exception: "+expression.toString());
        }
    }


    public  void buildExpression() throws ExpressionError {
        BinaryExpr bexp = null;
        try{
            bexp = unWrap(StaticJavaParser.parseExpression(expression));
        }catch (Throwable t){
            throw new ExpressionError(t.getMessage());
        }
        Set<String> variableNames = new HashSet<>();

        bexp.findAll(NameExpr.class).stream().forEach(new Consumer<NameExpr>() {
            @Override
            public void accept(NameExpr nameExpr) {
                variableNames.add(nameExpr.getName().toString());
            }
        });
        variables.addAll(variableNames);
    }

    public String buildClass(List<String> vars, String expression, String className){

        StringBuilder sb = new StringBuilder();
        String baseClassName = AbstractExpressionScript.class.getName();

        for(String imp: globalImports){
            sb.append(imp).append(";\n");
        }

        for(String imp: imports){
            sb.append(imp).append(";\n");
        }
        sb.append("public class ").append(className).append(" extends ").append(baseClassName).append(" implements Serializable {\n");
        for(String var: vars){
            sb.append("\tDouble ").append(var).append(";\n");
        }
        sb.append("\tpublic boolean expression() throws Throwable{\n");
        sb.append("\t\treturn (").append(expression).append(");\n");
        sb.append("\t}\n");
        sb.append("}");

        return sb.toString();
    }

    public IExpressionScript build() throws Throwable{
        buildExpression();
        IExpressionScript expressionScript = null;
        String className = "T_Sensor_"+ UUID.randomUUID().toString().replace("-","_");
        try {
            String sourceCode = buildClass(variables,expression,className);
            Interpreter it = new Interpreter();
            it.setClassLoader(getClass().getClassLoader());

            StringBuilder codeBuilder = new StringBuilder();
            codeBuilder.append(sourceCode).append("\n;");
            codeBuilder.append("Object obj=new "+className+"();");
            it.eval(codeBuilder.toString());
            Object obj = it.get("obj");
            expressionScript = (IExpressionScript) obj;

            ((AbstractExpressionScript)expressionScript).getVariables().addAll(variables);
        } catch (Throwable e) {
            throw e;
        }
        return expressionScript;
    }

    public static void main(String[] args) throws Throwable {

        String expression = "($(\"comsensor1%lot1-vv-chamber01\")>60)||((temp1+temp2)>110)&&offset(\"2p\")";
        SensorExpressionBuilder builder = new SensorExpressionBuilder();
        builder.expression = expression;
        IExpressionScript expressionScript = builder.build();
        System.out.println(expressionScript);

    }

}
