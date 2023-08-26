package com.hzw.fdc.engine.alg;


import java.math.BigDecimal;
import java.util.Arrays;

public class AlgorithmUtils {

    public static double mean(double... values){
        return Arrays.stream(values).summaryStatistics().getSum()/values.length;
    }

    public static double meanT(DataPoint... dataPoints){
        if(dataPoints.length<2){
            throw new RuntimeException("Length Must Greater Than 2");
        }
        Long t_i = null, t_i_1;
        Double v_i, v_i_1;
        Double sum = 0.0;
        for(int i=1;i<dataPoints.length;i++){
            t_i = dataPoints[i].time;
            t_i_1 = dataPoints[i-1].time;
            v_i = dataPoints[i].value;
            v_i_1 = dataPoints[i-1].value;
            sum = sum + ( ( v_i + v_i_1 ) * ( t_i - t_i_1 ) )/2;
        }
        //now t_i is the last points' time, and v_i is the last points' value;
        return sum/(t_i-dataPoints[0].time);
    }

    public static Double stdDevT(DataPoint... dataPoints){
        if(dataPoints.length<2){
            throw new RuntimeException("Length Must Greater Than 2");
        }

        BigDecimal part1 = new BigDecimal(0.0);
        BigDecimal part2 = new BigDecimal(0.0);
        BigDecimal dt = BigDecimal.valueOf(0l);
        BigDecimal part3 = new BigDecimal(0.0);
        BigDecimal fm2 = BigDecimal.valueOf(2);
        {
            BigDecimal t_i = null, t_i_1;
            BigDecimal v_i, v_i_1;
            BigDecimal sum = new BigDecimal(0.0);
            for(int i=1;i<dataPoints.length;i++){
                t_i = BigDecimal.valueOf(dataPoints[i].time);
                t_i_1 = BigDecimal.valueOf(dataPoints[i-1].time);
                v_i =  new BigDecimal(dataPoints[i].value);
                v_i_1 = new BigDecimal(dataPoints[i-1].value);
                sum = sum .add (
                        (( v_i .add( v_i_1) ).multiply ( t_i .subtract( t_i_1) )) .divide(fm2)
                );
            }
            dt = t_i .subtract(BigDecimal.valueOf(dataPoints[0].time)) ;
            part1 = ( sum .multiply( sum) ).divide(dt) ;
        }

        {
            BigDecimal t_i = null, t_i_1;
            BigDecimal v_i, v_i_1;
            BigDecimal sum = BigDecimal.valueOf(0.0);
            for(int i=1;i<dataPoints.length;i++){
                t_i = BigDecimal.valueOf(dataPoints[i].time);
                t_i_1 = BigDecimal.valueOf(dataPoints[i-1].time);
                v_i = BigDecimal.valueOf(dataPoints[i].value);
                v_i_1 = BigDecimal.valueOf(dataPoints[i-1].value);
                sum = sum.add (
                        (( v_i.pow(2).add( v_i_1.pow(2)) ).multiply ( t_i.subtract( t_i_1) )).divide(fm2)
                );
            }
            part2 = sum;

        }
        part3 =  (part2.subtract( part1)) .divide(dt);


        return Math.sqrt(part3.floatValue());


    }

    public static Double linearFit(DataPoint... dataPoints){
        return new LinearFitCalculator().linearFit(dataPoints);
    }

    /**
     * 新增的abs算法，PHASE5-65
     * PHASE5-31 Abs算法配置推送
     * @param value
     * @return
     */
    public static Double abs(double value){
        return Math.abs(value);
    }

    /**
     * 新增的area算法，PHASE5-59
     * PHASE5-30 area算法配置推送
     * @param dataPoints
     * @return
     */
    public static Double area(DataPoint... dataPoints) throws Exception{
        if(dataPoints==null){
            throw new Exception("wow=>[area算法参数异常][传入的参数为null]");
        }
        if(dataPoints.length<2){
            throw new Exception("wow=>[area算法参数异常][传入的数据点的数量小于2][length:"+dataPoints.length+"]");
        }
        double dt = 0;
        double dv = 0;
        double dArea = 0;
        double area = 0;
        for(int i=1;i<dataPoints.length;i++){
            dt = dataPoints[i].time/1000.0 - dataPoints[i-1].time/1000.0;
            dv = dataPoints[i].value + dataPoints[i-1].value;
            dArea = (dv*dt)/2;
            area = area + dArea;
        }
        return area;
    }


    public static class DataPoint{
        public long time;
        public double value;

        public DataPoint() {
        }

        public DataPoint(long time, double value) {
            this.time = time;
            this.value = value;
        }
    }


    public static void main(String[] args) {
        System.out.println(mean(1,2,3) == 2.0);
        System.out.println(meanT(new DataPoint[]{new DataPoint(1,10),new DataPoint(2,10)}));
        System.out.println(stdDevT(new DataPoint[]{new DataPoint(1,10),new DataPoint(2,10)}));

        System.out.println("linearFit Value: " + linearFit(new DataPoint[]{
                new DataPoint(1635829133129l,6990.531196d),
                new DataPoint(1635829205332l,6928.67697d),
                new DataPoint(1635829277528l,6982.998765d),
                new DataPoint(1635829349716l,6970.353497d),
                new DataPoint(1635829421907l,6996.138745d)
        }));
    }
}