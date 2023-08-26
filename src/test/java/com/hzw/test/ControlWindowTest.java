//package com.hzw.test;
//
//import com.alibaba.fastjson.JSON;
//import com.alibaba.fastjson.JSONArray;
//import com.alibaba.fastjson.JSONObject;
//import com.hzw.fdc.datagenerator.RawDataBuilder;
//import com.hzw.fdc.engine.api.ApiControlWindow;
//import com.hzw.fdc.engine.controlwindow.AbstractExpressionScript;
//import com.hzw.fdc.engine.controlwindow.ContainerWindow;
//import com.hzw.fdc.engine.controlwindow.ControlWindow;
//import com.hzw.fdc.engine.controlwindow.data.WindowIndicatorConfig;
//import com.hzw.fdc.engine.controlwindow.data.defs.IDataPacket;
//import com.hzw.fdc.engine.controlwindow.data.defs.ISensorData;
//import com.hzw.fdc.engine.controlwindow.data.impl.DataFlow;
//import com.hzw.fdc.engine.controlwindow.data.impl.DataPacket;
//import com.hzw.fdc.engine.controlwindow.data.impl.SensorData;
//import com.hzw.fdc.engine.controlwindow.data.impl.WindowClipResult;
//import com.hzw.fdc.engine.controlwindow.exception.ControlWindowException;
//import com.hzw.fdc.engine.controlwindow.exception.ControlWindowExpressionError;
//import com.hzw.fdc.engine.controlwindow.expression.WindowExpressionValidator;
//import com.hzw.fdc.engine.controlwindow.logistic.SensorExpressionBuilder;
//import com.hzw.fdc.scalabean.WindowData;
//import org.junit.Before;
//import org.junit.Test;
//import scala.collection.mutable.MutableList;
//
//import java.io.ByteArrayOutputStream;
//import java.io.FileInputStream;
//import java.io.IOException;
//import java.io.InputStream;
//import java.util.*;
//import java.util.function.Function;
//import java.util.stream.Collectors;
//
//public class ControlWindowTest {
//
//    private String windowDef = "";
//
//    @Before
//    public void prepareSourceCode() throws IOException {
////        InputStream is = ControlWindowTest.class.getClassLoader().getResourceAsStream("window-example-1.json");
////        ByteArrayOutputStream os = new ByteArrayOutputStream();
////        byte[] buffer = new byte[1024];
////        int readCount = 0;
////        while ((readCount = is.read(buffer)) > 0) {
////            os.write(buffer, 0, readCount);
////        }
////        is.close();
////        String content = os.toString();
////        windowDef = content;
//
//    }
//
//
//    @Test
//    public void logisticExpressionValidationTest(){
////        try {
////            ControlWindow cw = ControlWindow.parse(ControlWindow.TYPE_LogisticWindow,"a>1","!((a+1)>1a)");
////        } catch (ControlWindowExpressionError controlWindowExpressionError) {
////            controlWindowExpressionError.printStackTrace();
////        }
//
//       boolean f = WindowExpressionValidator.validateControlWindowExpression(ControlWindow.TYPE_LogisticWindow,"TestA-API>50","((a>1)+1)");
//        System.out.println(f);
//    }
//
//    @Test
//    public void f() throws IOException {
//
//
//        ControlWindow.addGlobalImport("import static com.hzw.fdc.engine.controlwindow.functions.HZWMaths.*");
//        //上面这句话让表达式里面支持这类里面的所有静态函数
//        //后面可以约定，表达式中可以是用我们提供的函数,且所有的函数都以$号开头，比如$pow(2,2)
//        // 考虑到安全因素，表达式中不允许Math.pow(x,y)这样通过"点"来引用的方式，仅允许变量表达式和函数直接调用，所有可用函数采用静态导入的方式配置并调用。
//
//        DataFlow dataFlow = buildDataFlowTestData1();
//
//        String s = JSON.toJSONString(dataFlow.toListData());
//
//        System.out.println(s);
//
//        /**
//         * ContainerWindow 是ControlWindow的子类，提供了包含子window的结构和计算框架，
//         * 由于我们目前的几类window都需要支持subWindow能力，因此都是继承至ContainerWindow，
//         *
//         */
//        ContainerWindow window = null;
//        ContainerWindow subWindow = null;
//        try {
//
//
////            window = (ContainerWindow) ControlWindow.parse(ControlWindow.TYPE_LogisticWindow,"(test>55)&&offset(\"Tb2p\")&&offset(\"Tb10p\")","(test>200)&&offset(\"Te2p\")");
//            window =(ContainerWindow) ControlWindow.parse(ControlWindow.TYPE_TimeBasedWindow,"Tb1p","Te2p");
//
//            window.setWindowId(1l);
//            window.addIndicatorConfig(new WindowIndicatorConfig(1l,1l,"test"));
//
//
//
////
//            subWindow =  (ContainerWindow) ControlWindow.parse(ControlWindow.TYPE_CycleWindowMin,"Tb0p","Te0p_(?)");
//            subWindow.setWindowId(2l);
//            subWindow.addIndicatorConfig(new WindowIndicatorConfig(1l,1l,"test"));
//            subWindow.addIndicatorConfig(new WindowIndicatorConfig(1l,1l,"test"));
//////
//            window.addSubWindow(subWindow);
//
//            window.attachDataFlow(dataFlow);
//
//
//
//            try {
//                MutableList<WindowData> windowClipResults = ApiControlWindow.calculate(window);
//                System.out.println("------->>>>>>>>>");
//                System.out.println(window.getSnapShotInfo("ccccc11"));
////                System.out.println("result: "+windowClipResults);
//
//                window.attachDataFlow(dataFlow);
//                windowClipResults = ApiControlWindow.calculate(window);
//
////                System.out.println("result: "+windowClipResults);
//            } catch (ControlWindowException e) {
//                e.printStackTrace();
//            }
//
////            window.attachDataFlow(dataFlow);
////
////            try {
////                Collection<WindowClipResult> windowClipResults = window.calculate();
////
////                System.out.println("result: "+windowClipResults);
////            } catch (ControlWindowException e) {
////                e.printStackTrace();
////            }
//
//
//        } catch (ControlWindowExpressionError controlWindowExpressionError) {
//            controlWindowExpressionError.printStackTrace();
//        }
//
//
//    }
//
//
//    public static String provideData1() throws IOException {
//        StringBuilder sb = new StringBuilder();
//        String json = readFile("./data1-cycle.json");
//        JSONArray array = JSONArray.parseArray(json);
//        for(int i=0;i<array.size();i++){
//            JSONObject object = array.getJSONObject(i);
//            sb.append(object.getInteger("runProcessTime")).append("\t")
//                    .append(object.getInteger("stepNo")).append("\t")
//                    .append(Double.parseDouble(object.getString("sensorValue"))).append("\n");
//        }
//        return sb.toString();
//    }
//
//    public static String readFile(String file) throws IOException {
//        InputStream fileReader = new FileInputStream(file);
//        ByteArrayOutputStream bos = new ByteArrayOutputStream();
//        byte[] buffer = new byte[1024];
//        int len = -1;
//        while (( len = fileReader.read(buffer))>0){
//            bos.write(buffer,0,len);
//        }
//        fileReader.close();
//        return bos.toString("utf-8");
//    }
//
//    /**
//     *
//     * @return
//     */
//
//    public static String provideData(){
//
//        StringBuilder sb = new StringBuilder();
//        int i = 1;
//        long d = 1630047881857l;
//        for( i=1;i<5;i++){
//            sb.append(d+i*1000).append(",").append(1).append(",").append(i).append("\n");
//        }
//
//        for( i=5;i<7;i++){
//            sb.append(d+i*1000).append(",").append(2).append(",").append(i).append("\n");
//        }
//
//        for( i=7;i<17;i++){
//            sb.append(d+i*1000).append(",").append(3).append(",").append(i).append("\n");
//        }
//
//        for( i=17;i<67;i++){
//            sb.append(d+i*1000).append(",").append(4).append(",").append(i).append("\n");
//        }
//
//        for( i=67;i<72;i++){
//            sb.append(d+i*1000).append(",").append(5).append(",").append(i).append("\n");
//        }
//
//        for( i=72;i<152;i++){
//            sb.append(d+i*1000).append(",").append(6).append(",").append(i).append("\n");
//        }
//
//        for( i=152;i<154;i++){
//            sb.append(d+i*1000).append(",").append(8).append(",").append(i).append("\n");
//        }
//
//
////        for(int i=33;i<35;i++){
////            sb.append(i*1000).append(",").append(42).append(",").append(i).append("\n");
////        }
//
////        for(int i=35;i<36;i++){
////            sb.append(i*1000).append(",").append(49).append(",").append(i).append("\n");
////        }
//
//        return sb.toString();
//
////        return ""+"1626935430666,1,60.0,\n" +
////                "1626935431666,2,61.0,\n" +
////                "1626935432666,3,62.0,\n";
//
//    }
//
//    public static DataFlow buildDataFlowData(){
//        DataFlow dataFlow = DataFlow.build();
//        String s = "(1622099499332,88.0,1| (1622099500332,88.0,2| (1622099501333,88.0,7| (1622099519337,88.0,9| (1622099524338,88.0,9| (1622099518336,88.0,9| (1622099531339,88.0,9| (1622099516336,88.0,9| (1622099507334,88.0,9| (1622099522337,88.0,9| (1622099502333,88.0,9| (1622099521337,88.0,9| (1622099506334,88.0,9| (1622099535340,88.0,9| (1622099525338,88.0,9| (1622099517336,88.0,9| (1622099533340,88.0,9| (1622099504333,88.0,9| (1622099514335,88.0,9| (1622099530339,88.0,9| (1622099511335,88.0,9| (1622099509334,88.0,9| (1622099537341,88.0,9| (1622099505334,88.0,9| (1622099534340,88.0,9| (1622099538341,88.0,9| (1622099512335,88.0,9| (1622099536340,88.0,9| (1622099510335,88.0,9| (1622099527338,88.0,9| (1622099529339,88.0,9| (1622099515336,88.0,9| (1622099526338,88.0,9| (1622099528339,88.0,9| (1622099508334,88.0,9| (1622099503333,88.0,9| (1622099532339,88.0,9| (1622099523337,88.0,9| (1622099513335,88.0,9| (1622099520337,88.0,9| (1622099556345,88.0,10| (1622099544342,88.0,10| (1622099550343,88.0,10| (1622099543342,88.0,10| (1622099551344,88.0,10| (1622099540341,88.0,10| (1622099539341,88.0,10| (1622099552344,88.0,10| (1622099555344,88.0,10| (1622099554344,88.0,10| (1622099545342,88.0,10| (1622099557345,88.0,10| (1622099546342,88.0,10| (1622099541341,88.0,10| (1622099542342,88.0,10| (1622099558345,88.0,10| (1622099553344,88.0,10| (1622099549343,88.0,10| (1622099547343,88.0,10| (1622099548343,88.0,10)";
//
//        //        Pattern p = Pattern.compile("(\\(.*\\)[, ])");
////        Matcher matcher = p.matcher(s);
////        while (matcher.find()){
////            int gc = matcher.groupCount();
////            for(int i=1;i<=gc;i++){
////                System.out.println(matcher.group(i));
////            }
////        }
//
//        String[] ts = s.split("\\|");
//        List<String> as = new ArrayList<>();
//        for(String t: ts){
//            IDataPacket dp = DataPacket.build();
//            String[] pp = t.replace("(","").replace(")","").split(",");
//            dp.setTimestamp(Long.parseLong(pp[0].trim()));
//            try {
//                dp.setStepId(Integer.parseInt(pp[2].trim()));
//            }catch (Throwable t1){
//                t1.printStackTrace();
//                System.out.println(t);
//            }
//            ISensorData s1 = SensorData.build();
//            s1.setTimestamp(Long.parseLong(pp[0].trim()));
//            s1.setValue(Double.parseDouble(pp[1].trim()));
//            s1.setStepId(Integer.parseInt(pp[2].trim()));
//            s1.setSensorName("test");
//            s1.setSensorAlias("test");
//            dp.getSensorDataMap().put(s1.getSensorAlias(),s1);
//            dataFlow.getDataList().add(dp);
//        }
//
//        return dataFlow;
//    }
//
//
//    public static DataFlow buildDataFlowTestData1() throws IOException {
//        String s = provideData();
//
//        System.out.println("raw data:");
//        System.out.println(s);
//
//
//        DataFlow dataFlow = DataFlow.build();
//       String[] ts = s.split("\n");
//       for(String t: ts){
//           if(t.trim().equals("")) continue;
//           String[] p = t.split(",");
//
//           IDataPacket dp = DataPacket.build();
//           long t1 = Long.parseLong(p[0]);
//           dp.setTimestamp(t1);
//           try{
//               dp.setStepId(Integer.parseInt(p[1].trim()));
//           }catch (Throwable vv){
//               vv.printStackTrace();
//           }
//
//
//           SensorData s1 = SensorData.build();
//           s1.setSensorName("test");
//           s1.setValue(Double.parseDouble(p[2].trim()));
//            s1.setStepId(dp.getStepId()); // mark
//           s1.setSensorAlias("test");
//           s1.setTimestamp(t1);
//
//           dp.getSensorDataMap().put(s1.getSensorAlias(),s1);
//
////
////            s1 = new SensorData();
////           s1.setSensorName("test1");
////           s1.setValue(Double.parseDouble(p[1].trim()));
////           s1.setStepId(dp.getStepId()); // mark
////           s1.setSensorAlias("test1");
////           s1.setTimestamp(t1);
////
////           dp.getSensorDataMap().put(s1.getSensorAlias(),s1);
//
//           dataFlow.getDataList().add(dp);
//
//       }
//
//
//
//       return dataFlow;
//    }
//
//    public static DataFlow buildDataFlowTestData(){
//        DataFlow dataFlow = DataFlow.build();
//
//        int packetCount = 103;
//        for(int i=0;i<packetCount;i++){
//
//            DataPacket dp = DataPacket.build();
//            long t = System.currentTimeMillis() + 100;
//            int stepId = i%10;
//
//            dp.setTimestamp(t);
//            dp.setStepId(stepId);
//
//            SensorData s1 = SensorData.build();
//            s1.setSensorName("s1");
//            s1.setValue(i);
////            s1.setStepId(stepId); // mark
//            s1.setSensorAlias("sensor1");
//            s1.setTimestamp(t);
//            dp.getSensorDataMap().put(s1.getSensorAlias(),s1);
//
//            SensorData stepIdSensor = SensorData.build();
//            stepIdSensor.setValue(stepId);
////            stepIdSensor.setStepId(stepId);
//            stepIdSensor.setSensorAlias("stepId");
//            stepIdSensor.setSensorName("stepId");
//            stepIdSensor.setTimestamp(t);
//            dp.getSensorDataMap().put(stepIdSensor.getSensorAlias(),stepIdSensor);
//
//            dataFlow.getDataList().add(dp);
//        }
//        return dataFlow;
//    }
//
//    @Test
//    public void t1() throws InterruptedException {
//
//
////        Thread.sleep(10000000);
//    }
//
//
//    @Test
//    public void t2(){
//        String s = "(1,2,3,4,?,-5,6,*)";
////        Pattern pattern = Pattern.compile("(\\b\\d+\\b)");
////        Matcher matcher = pattern.matcher(s);
////        while (matcher.find()){
////            int gc = matcher.groupCount();
////            for(int i=0;i<gc;i++){
////                System.out.println(matcher.group(i));
////            }
////        }
//
//
//       String p = start(s);
//        System.out.println(p);
//
//    }
//
//    public String start(String p){
//        p = p.replace("(","").replace(")","");
//        String[] s = p.split(",");
//        List<String> l = Arrays.stream(s).map(new Function<String, String>() {
//            @Override
//            public String apply(String s) {
//                s = s.trim();
//                if("?".equals(s)){
////                    return "singleNumber()";
//                    return singleNumberPattern();
//                }else if("*".equals(s)){
////                    return "anyNumbers()";
//                    return anyNumbers();
//                }else if(s.startsWith("[")&&s.endsWith("]")){
//                        String[] p  = s.replace("[","(").replace("]",")").split("-");
//                        return rangePattern(Integer.parseInt(p[0].trim()),Integer.parseInt(p[1].trim()));
//                }else{
//                        return s;
//                }
//            }
//
//            private String singleNumberPattern(){
//                return "[,]?[-]?\\b\\d+\\b[,]?";
//            }
//
//            private String anyNumbers(){
//                return "([,]?[-]?\\b\\d+\\b[,]?)*";
//            }
//
//            private String rangePattern(int start, int end){
//                StringBuilder sb = new StringBuilder();
//                for(int i=start;i<=end;i++){
//                    sb.append("[,]?"+start+"[,]?");
//                }
//                return sb.toString();
//            }
//
//
//        }).filter(s1->s1!=null).collect(Collectors.toList());
//        System.out.println(l);
//        StringBuilder sb = new StringBuilder();
//        for(int i=0;i<l.size();i++){
//            sb.append(l.get(i)).append(",");
//        }
//        sb.deleteCharAt(sb.length()-1);
//        return sb.toString();
//
//    }
//
//
//    public void f(List<String> l){
//
//    }
//
//
//
//
//    @Test
//    public void testF() throws IOException {
////        ControlWindow.addGlobalImport("import static com.hzw.fdc.engine.controlwindow.functions.HZWMaths.*");
//        //上面这句话让表达式里面支持这类里面的所有静态函数
//        //后面可以约定，表达式中可以是用我们提供的函数,且所有的函数都以$号开头，比如$pow(2,2)
//        // 考虑到安全因素，表达式中不允许Math.pow(x,y)这样通过"点"来引用的方式，仅允许变量表达式和函数直接调用，所有可用函数采用静态导入的方式配置并调用。
//
//        DataFlow dataFlow = (DataFlow) RawDataBuilder.loadDataFlow("t.data");
//        dataFlow.setStartTime(dataFlow.getDataList().get(0).getTimestamp());
//        dataFlow.setStopTime(dataFlow.getDataList().get(dataFlow.getDataList().size()-1).getTimestamp());
//
//        String s = JSON.toJSONString(dataFlow.toListData());
//
////        System.out.println(s);
//
//        /**
//         * ContainerWindow 是ControlWindow的子类，提供了包含子window的结构和计算框架，
//         * 由于我们目前的几类window都需要支持subWindow能力，因此都是继承至ContainerWindow，
//         *
//         */
//        ContainerWindow window = null;
//        ContainerWindow subWindow = null;
//        try {
//
//
//
////            window = (ContainerWindow) ControlWindow.parse(ControlWindow.TYPE_StepIdBasedWindow,"S14b1p","S14b2p");
////            window = (ContainerWindow) ControlWindow.parse(ControlWindow.TYPE_RSNStepIdBasedWindow,"C1S9b1p","C1S9e1p");
////            window = (ContainerWindow) ControlWindow.parse(ControlWindow.TYPE_TimeBasedWindow,"Tb10p","Te0p");
//
////            window = (ContainerWindow) ControlWindow.parse(ControlWindow.TYPE_LogisticWindow, "{(CMPSEN002>22&&StepID==4)}","{(CMPSEN002==500)}");
////            window = (ContainerWindow) ControlWindow.parse(ControlWindow.TYPE_TimeBasedWindow, "Tb0p","Te1p");
//            window = (ContainerWindow) ControlWindow.parse(ControlWindow.TYPE_StepIdBasedWindow,"S2b1p","S3e3p");
////            window = (ContainerWindow) ControlWindow.parse(ControlWindow.TYPE_TimeBasedWindow,"Tb10p","Te10p");
////            subWindow = (ContainerWindow) ControlWindow.parse(ControlWindow.TYPE_CycleWindowMin, "Tb0p","Te10S_(1,*,3)");
//
//            window.setWindowId(1l);
//            window.addIndicatorConfig(new WindowIndicatorConfig(1l,1l,"Ar_B_flow"));
////            subWindow.setWindowId(2l);
////            subWindow.addIndicatorConfig(new WindowIndicatorConfig(2l,1l,"Ar_B_flow"));
//
////            subWindow = new CycleWindow("(?,*,[1-4])",false,"Tb0p","Te0p");
////            subWindow = (ContainerWindow) ControlWindow.parse(ControlWindow.TYPE_TimeBasedWindow,"Tb0p","Te0p");
////            subWindow.setWindowId(2l);
////            subWindow.addIndicatorConfig(new WindowIndicatorConfig(2l,1l,"test"));
////
////            window.addSubWindow(subWindow);
//
//            window.attachDataFlow(dataFlow);
////            window.calculate();
//
//            window.getRequiredSensorAlias();
//
//
//
//            try {
//                Collection<WindowClipResult> windowClipResults = window.calculate();
//
//                List<WindowClipResult> all = new ArrayList<>();
//                all.addAll(windowClipResults);
//                Collections.sort(all, new Comparator<WindowClipResult>() {
//                    @Override
//                    public int compare(WindowClipResult o1, WindowClipResult o2) {
//                        return o1.getCycleUnitIndex()-o2.getCycleUnitIndex();
//                    }
//                });
//                System.out.println("result: "+windowClipResults);
////                TestUI.showDataFlowData(dataFlow,all);
//            } catch (ControlWindowException e) {
//                e.printStackTrace();
//            }
//
//
//        } catch (ControlWindowExpressionError controlWindowExpressionError) {
//            controlWindowExpressionError.printStackTrace();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//    }
//
//    @Test
//    public void f9(){
//        String exp = "(exp1>1)&&(a2<34)";
//        try {
//            AbstractExpressionScript script = (AbstractExpressionScript) new SensorExpressionBuilder(exp).build();
//
//            System.out.println(script.getVariables());
//
//        } catch (Throwable throwable) {
//            throwable.printStackTrace();
//        }
//    }
//
//    public static void main(String[] args) {
//
//    }
//}
