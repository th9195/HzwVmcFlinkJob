//package com.hzw.indicatortest;
//
//import com.hzw.fdc.engine.calculatedindicator.CalculatedIndicatorBuildException;
//import com.hzw.fdc.engine.calculatedindicator.CalculatedIndicatorBuilder;
//import com.hzw.fdc.engine.calculatedindicator.CalculatedIndicatorBuilderFactory;
//import com.hzw.fdc.engine.calculatedindicator.ICalculatedIndicator;
//import org.junit.Before;
//import org.junit.Test;
//
//import java.io.ByteArrayOutputStream;
//import java.io.IOException;
//import java.io.InputStream;
//import java.util.HashMap;
//import java.util.Map;
//
//public class CalculatedIndicatorTest {
//
//    private String sourceCode = "";
//
//    @Before
//    public void prepareSourceCode() throws IOException {
//        InputStream is = CalculatedIndicatorTest.class.getClassLoader().getResourceAsStream("example-calculated-indicator3.jscript");
//        ByteArrayOutputStream os = new ByteArrayOutputStream();
//        byte[] buffer = new byte[1024];
//        int readCount = 0;
//        while ((readCount = is.read(buffer)) > 0) {
//            os.write(buffer, 0, readCount);
//        }
//        is.close();
//        String csvContent = os.toString();
//        sourceCode = csvContent;
//
//    }
//
//    @Test
//    public void test() {
//
//
//        Map<String,Double> indicatorValues = new HashMap<>();
//        indicatorValues.put("S1b6p$S1e3p%HDMS_flow%MeanT",0.0);
////        indicatorValues.put("All Time%CMPSEN001%Sum",0.0);
////        indicatorValues.put("indicator3",0.0);
////        indicatorValues.put("indicator4",0.0);
//
////        try {
////            Double ret = LogisticIndicatorTester.test(sourceCode,indicatorValues);
////
////            System.out.println(ret); //
////
////        } catch (Throwable throwable) {
////            throwable.printStackTrace();
////            return;
////        }
//
//
//        CalculatedIndicatorBuilder builder = CalculatedIndicatorBuilderFactory.makeBuilder();
//        builder.setCalculatedIndicatorClassName("All Time%CMPSEN001%StdDevT");
//
//        builder.setAllowLoop(true);
//
//        builder.setImplementation(sourceCode);
//
//        builder.setTimeLimit(500);
//
//        builder.setUserTimeLimit(300);
//
//        ICalculatedIndicator calculatedIndicator = null;
//
//        try {
//            calculatedIndicator = builder.build();
//        } catch (CalculatedIndicatorBuildException e) {
//            e.printStackTrace();
//        }
//
//        try {
//            if (calculatedIndicator == null) return;
//
//
//
//            calculatedIndicator.setIndicatorValue("98023" ,"All $Time$BCT_Temp_Control_flow%Average",0d);
//
//
//
//
//
//            double calculatedIndicatorValue = calculatedIndicator.calculate();
//
//            System.out.println("calculated indicator value: " + calculatedIndicatorValue);
//
//        } catch (Throwable t) {
//            t.printStackTrace();
//        } finally {
//            calculatedIndicator = null;
//        }
//    }
//
//
//}
