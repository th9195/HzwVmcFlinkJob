package com.hzw.fdc.engine.calculatedindicator;

import com.github.javaparser.StaticJavaParser;
import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.ast.expr.Expression;
import com.github.javaparser.ast.expr.StringLiteralExpr;
import com.github.javaparser.ast.stmt.*;
import groovy.lang.Binding;
import groovy.lang.GroovyShell;

import javax.tools.Diagnostic;
import javax.tools.JavaFileObject;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.function.Consumer;
import java.util.regex.Pattern;


public class CalculatedIndicatorBuilder {
    private Map<String, Double> indicatorVariableMap = new HashMap<>();
    private Map<String, String> indicatorIdVarMap = new HashMap<>();
    private List<String> imports = new ArrayList<>();
    private List<String> staticImports = new ArrayList<>();
    private String name = null;
    private String calculateImplementationStr = null;
    private boolean allowLoop = false;
    private Set<String> keywordBlackList = new HashSet<>();
    static Pattern forPattern = Pattern.compile("([ ]*for[ ]*\\{.*)");
    static Pattern whilePattern = Pattern.compile("([ ]*while[ ]*\\{.*)");
    static Pattern doPattern = Pattern.compile("([ ]*do[ ]*\\{.*)");
    private String sourceCode = null;
    private long timeLimit = 500;
    private long userTimeLimit = 500;
    private static String SOURCE_TAG="public double _userImplementation() throws Throwable{";

    public CalculatedIndicatorBuilder() {
        keywordBlackList.addAll(Arrays.asList(
            "Thread","ThreadGroup","Runtime","System","SecurityManager","RuntimePermission","ProcessBuilder","Process","ProcessBuilder.Redirect"
            ,"Package","Number","InheritableThreadLocal","Enum","Compiler","ClassValue","ClassLoader","StackTraceElement","Void",
            "java.lang.management","java.lang.ref","java.lang.annotation","java.lang.instrument","java.lang.invoke",
            "java.awt","java.applet","java.","com.","antlr.","io.","javax.","net.","tk.","java.util.","org.","Class","this"
        ));
    }


    public CalculatedIndicatorBuilder setAllowLoop(boolean allowLoop){
        this.allowLoop = allowLoop;
        return this;
    }

    public CalculatedIndicatorBuilder setCalculatedIndicatorClassName(String name){
        this.name = name;
        return this;
    }

    public CalculatedIndicatorBuilder setTimeLimit(long timeLimit){
        this.timeLimit = timeLimit;
        return this;
    }

    public CalculatedIndicatorBuilder setUserTimeLimit(long userTimeLimit){
        this.userTimeLimit = userTimeLimit;
        return this;
    }


    public CalculatedIndicatorBuilder setImplementation(String implementation){
        this.calculateImplementationStr = implementation;
        return this;
    }

    public CalculatedIndicatorBuilder addImport(String importElement){
        this.imports.add(importElement);
        return this;
    }

    public CalculatedIndicatorBuilder addStaticImport(String importElement){
        this.staticImports.add(importElement);
        return this;
    }



    public String getMd5ClassName(){
        String nameMd5 = stringToMD5(name);
        String className = "DyClass_"+nameMd5;
        return className;
    }

    public ICalculatedIndicator build() throws CalculatedIndicatorBuildException {
        validateImplementationPolicy();
        try {
            ICalculatedIndicator calculatedIndicator = null;
            this.sourceCode = generateSourceCode();


            Binding binding = new Binding();
            GroovyShell shell = new GroovyShell(binding);
            StringBuilder codeBuilder = new StringBuilder();
            codeBuilder.append(sourceCode).append("\n;");
            codeBuilder.append(" obj=new "+getMd5ClassName()+"();");

            shell.evaluate(codeBuilder.toString());
            Object obj = binding.getVariable("obj");

            calculatedIndicator = (ICalculatedIndicator)obj;
            return calculatedIndicator;
        } catch (Throwable e) {
            throw new CalculatedIndicatorBuildException(e.getMessage(),e);
        }

    }

    private void validateImplementationPolicy() throws CalculatedIndicatorBuildException{
        if(calculateImplementationStr==null) throw new CalculatedIndicatorBuildException("implementation source code null");

        //检查是否violate了黑名单关键词
        for(String keyword: keywordBlackList){
            if(calculateImplementationStr.contains(keyword)){
                throw new CalculatedIndicatorBuildException("implementation source code contains forbidden keywords: "+keyword);
            }
        }

        //检查是否有for、while等表达式
        if(!allowLoop){
            if(forPattern.matcher(calculateImplementationStr).find()||doPattern.matcher(calculateImplementationStr).find()||whilePattern.matcher(calculateImplementationStr).find()){
                throw new CalculatedIndicatorBuildException("implementation source code contains loop which is not allowed");
            }
        }

        String rawSourceCode = null;
        int codeRowStart = 0;
        String className = "ClassCompileTest_"+System.currentTimeMillis();
        try {
             rawSourceCode = prepareCalculatedIndicatorSourceCode(className);
             String[] codeLines = rawSourceCode.split("\n");

             for(String codeLine: codeLines){
                 codeRowStart++;
                 if(codeLine.contains(SOURCE_TAG)){
                     break;
                 }
             }
             codeRowStart++;
//            InMemoryCompileEngine.compileTest(className,rawSourceCode);
        } catch (Throwable throwable) {
            if(throwable instanceof CompileTestException){
                CompileTestException cte = (CompileTestException) throwable;
                List<Diagnostic<? extends JavaFileObject>> diagnosticList = cte.getList();
                StringBuilder errorMsg = new StringBuilder();
                for(Diagnostic diagnostic: diagnosticList){
                    try {
                        String msg = diagnostic.toString();
                        msg = msg.replace("/" + className + ".java:", "");
                        String lineNumberStr = msg.substring(0, msg.indexOf(":"));
                        int lineNumber = Integer.parseInt(lineNumberStr);
                        msg = msg.substring(msg.indexOf(":") + 1);
                        msg = "line " + (lineNumber - codeRowStart) + " column " + diagnostic.getColumnNumber() + " : " + msg;
                        errorMsg.append(msg).append("\n");
                    }catch (Throwable t){

                    }
                }
                throw new CalculatedIndicatorCompileException(errorMsg.toString());
            }
            throw new CalculatedIndicatorBuildException(throwable.getMessage());
        }

        return;
    }

    private String prepareCalculatedIndicatorSourceCode(String className) {
        StringBuilder sb = new StringBuilder();

        //add imports
        for(String importElement: imports){
            sb.append("import ").append(importElement).append(";\n");
        }

        //add static imports
        for(String staticImportElement: staticImports){
            sb.append("import static ").append(staticImportElement).append(".*;\n");
        }

        sb.append("public class ").append(className).append(" extends ").append(AbstractCalculatedIndicator.class.getName()).append(" implements Serializable {\n");


        //生成Indicator变量
        for(String varName: indicatorVariableMap.keySet()){
            sb.append("\tprivate Double ").append(varName).append(" = ").append(indicatorVariableMap.get(varName)).append(";\n");
        }

        //生成构造函数
        sb.append("\tpublic ").append(className).append("(){\n");
        for(String indicatorId: indicatorIdVarMap.keySet()){
            sb.append("\t\tindicatorIdVariableMapping.put(\"").append(indicatorId).append("\",\"").append(indicatorIdVarMap.get(indicatorId)).append("\");\n");
        }
        sb.append("\t\tsetTimeLimit(").append(Math.min(timeLimit,userTimeLimit)).append(");\n");
        sb.append("\t}\n");


        //生成实现算法
        sb.append("\tpublic double _userImplementation() throws Throwable{\n");
        sb.append("\t\ttry{\n");
        sb.append(this.calculateImplementationStr).append("\n");
        sb.append("\t\t}catch(Throwable t){\t\n");
        sb.append("\t\t\tthrow t;\n");
        sb.append("\t\t}\n");
        sb.append("\t}\n");

        sb.append("}");
        return sb.toString();
    }

    private String postProcessGeneratedSourceCode(String sc){
        CompilationUnit cu = StaticJavaParser.parse(sc);


        cu.findAll(ForStmt.class).stream().forEach(new Consumer<ForStmt>() {
            @Override
            public void accept(ForStmt forStmt) {
                BlockStmt blockStmt = forStmt.getBody().asBlockStmt();
                blockStmt.addStatement(0,StaticJavaParser.parseStatement("timeLimitValidation();"));
            }
        });
        cu.findAll(WhileStmt.class).stream().forEach(new Consumer<WhileStmt>() {
            @Override
            public void accept(WhileStmt stmt) {
                BlockStmt blockStmt = stmt.getBody().asBlockStmt();
                blockStmt.addStatement(0,StaticJavaParser.parseStatement("timeLimitValidation();"));
            }
        });
        cu.findAll(StringLiteralExpr.class).stream().forEach(new Consumer<StringLiteralExpr>() {
            @Override
            public void accept(StringLiteralExpr statement) {

                Expression stringCreateExp  = StaticJavaParser.parseExpression("toJavaString(\""+Base64.getEncoder().encodeToString(statement.asString().getBytes(StandardCharsets.UTF_8))+"\")");

                Object o = statement.getParentNode();
                statement.setString(statement.asString().replaceAll("\\$","\\$"));
                if(statement.hasParentNode()){
                    statement.getParentNode().get().replace(statement,stringCreateExp);
                }


            }
        });

        cu.findAll(DoStmt.class).stream().forEach(new Consumer<DoStmt>() {
            @Override
            public void accept(DoStmt stmt) {
                BlockStmt blockStmt = stmt.getBody().asBlockStmt();
                blockStmt.addStatement(0,StaticJavaParser.parseStatement("timeLimitValidation();"));
            }
        });

        cu.findAll(ForEachStmt.class).stream().forEach(new Consumer<ForEachStmt>() {
            @Override
            public void accept(ForEachStmt stmt) {
                BlockStmt blockStmt = stmt.getBody().asBlockStmt();
                blockStmt.addStatement(0,StaticJavaParser.parseStatement("timeLimitValidation();"));
            }
        });


        return cu.toString();
    }

    private String generateSourceCode() throws Throwable{

        String sc =  prepareCalculatedIndicatorSourceCode(getMd5ClassName());
        System.out.println("source code:\n"+sc);
        return postProcessGeneratedSourceCode(sc);
    }

    public String getSourceCode() {
        return sourceCode;
    }

    public static String stringToMD5(String plainText) {
        byte[] secretBytes = null;
        try {
            secretBytes = MessageDigest.getInstance("md5").digest(
                    plainText.getBytes());
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("没有这个md5算法！");
        }
        String md5code = new BigInteger(1, secretBytes).toString(16);
        for (int i = 0; i < 32 - md5code.length(); i++) {
            md5code = "0" + md5code;
        }
        return md5code;
    }
}
