package com.hzw.fdc.engine.calculatedindicator;

import javax.naming.OperationNotSupportedException;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 *  可编程CalculatedIndicator的抽象类。用户自定义编程的实现继承至该类，由CalculatedIndicatorBuilder自动生成。
 *
 */
public abstract class AbstractCalculatedIndicator implements ICalculatedIndicator {

    protected Map indicatorIdVariableMapping = new Map();
    protected Map indicatorAliasVariableMapping = new Map();
    protected Map indicatorAliasIdMapping = new Map();
    protected java.util.List<String> indicatorList = new List();
    protected long timeLimit = 500;
    long startTime = 0;

    public AbstractCalculatedIndicator $ = null;


    public AbstractCalculatedIndicator() {
        $ = this;
    }

    @Override
    public void setIndicatorValue(String indicatorId, String alias,  Double value) throws IndicatorIdNotMappedException{
        indicatorIdVariableMapping.put(indicatorId,value);
        indicatorAliasVariableMapping.put(alias,value);
        indicatorAliasIdMapping.put(alias,indicatorId);
        if(!indicatorList.contains(alias)){
            indicatorList.add(alias);
        }
    }

    public void setTimeLimit(long timeLimit){
        this.timeLimit = timeLimit;
    }

    public abstract double _userImplementation() throws Throwable;

    @Override
    public double calculate() throws Throwable {
        startTime = System.currentTimeMillis();
        double d = _userImplementation();
        return d;
    }

    protected  void timeLimitValidation(){
        long elapsedTime = (System.currentTimeMillis()-startTime);
        if(elapsedTime>timeLimit){
            throw new IndicatorTimeLimitExceedException(elapsedTime,timeLimit);
        }
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
    }

    protected Double $(String alias) throws Throwable{
        if(!indicatorAliasVariableMapping.containsKey(alias)){
            throw new IndicatorIdNotMappedException(alias);
        }else{
            return (Double) indicatorAliasVariableMapping.get(alias);
        }
    }

    protected String id(String alias) throws Throwable{
        if(!indicatorAliasIdMapping.containsKey(alias)){
            throw new IndicatorIdNotMappedException(alias);
        }else{
            return (String) indicatorAliasIdMapping.get(alias);
        }
    }


    protected AbstractCalculatedIndicator $(){
        return this;
    }

    protected Object $(Object object) throws Throwable {

        if(object instanceof String){
            return $((String)object);
        }else if((object instanceof AbstractCalculatedIndicator)&&(object==this)){
            return this;
        }else{
            throw new Exception("Not Supported Operation");
        }
    }

    protected String toJavaString(String base64String){
        return new String(Base64.getDecoder().decode(base64String.getBytes(StandardCharsets.UTF_8)));
    }

    protected Map info(String alias) throws Throwable{
        throw new OperationNotSupportedException();
    }

    protected void print(Object objects){
        System.out.println(objects);
    }

    protected String[] indicators(){
        String[] array = new String[indicatorList.size()];
        array = indicatorList.toArray(array);
        return array;
    }


    public  Object[] array(Object... t){
        return t;
    }

    public List list(){
        return new List();
    }

    public List list(Object... args){
        return new List(Arrays.asList(args));
    }

    public Set set(){
        return new Set();
    }

    public Set set(Object... args){
        return new Set(Arrays.asList(args));
    }

    public Map map(){
        return new Map();
    }

    public static class List<T> extends ArrayList<T>{
        public List(int initialCapacity) {
            super(initialCapacity);
        }

        public List() {
        }

        public List(Collection<? extends T> c) {
            super(c);
        }
    }

    public static class Set extends HashSet{
        public Set() {
        }

        public Set(Collection c) {
            super(c);
        }

        public Set(int initialCapacity, float loadFactor) {
            super(initialCapacity, loadFactor);
        }

        public Set(int initialCapacity) {
            super(initialCapacity);
        }
    }

    public static class Map extends HashMap{}




}
