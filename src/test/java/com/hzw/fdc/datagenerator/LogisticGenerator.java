package com.hzw.fdc.datagenerator;

/**
 *  y=a*x+b
 */
public class LogisticGenerator extends DataFlowPartGenerator{
     double k = 50;
     double p = 100;
     double r = 1;


    public double f(double t){
        return (k*p*Math.pow(Math.E,r*t))/(k+p*(Math.pow(Math.E,r*t)-1));
    }
}
