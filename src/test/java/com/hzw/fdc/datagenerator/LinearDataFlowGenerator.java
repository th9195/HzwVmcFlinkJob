package com.hzw.fdc.datagenerator;

/**
 *  y=a*x+b
 */
public class LinearDataFlowGenerator extends DataFlowPartGenerator{
     double a=1;
     double b=0;


    public double f(double x){
        return a*x+b;
    }
}
