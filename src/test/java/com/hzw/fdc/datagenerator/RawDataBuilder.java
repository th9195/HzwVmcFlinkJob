package com.hzw.fdc.datagenerator;

import com.hzw.fdc.engine.controlwindow.data.defs.IDataFlow;
import com.hzw.fdc.engine.controlwindow.data.impl.DataFlow;
import com.hzw.fdc.engine.controlwindow.data.impl.DataPacket;
import com.hzw.fdc.engine.controlwindow.data.impl.SensorData;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class RawDataBuilder {

    public static IDataFlow loadDataFlow(String file) throws IOException {
        List<String> lines = FileUtils.readLines(new File(file));
        String header = lines.get(0);
        String[] columns = header.split("[\t ]");
        DataFlow dataFlow = new DataFlow();
        Long startTime = 0l;
        for(int i=1;i<lines.size();i++){
            String row = lines.get(i);
            String[] cs = row.split("[ \t]");
            Long timestamp = Long.parseLong(cs[0].split("--")[2]);
            startTime = timestamp;
            long sec  = (long) (Double.parseDouble(cs[1])*1000);
            timestamp = timestamp+sec;


            Integer stepId = Integer.parseInt(cs[cs.length-1]);

            DataPacket dataPacket = new DataPacket();
            dataPacket.setStepId(stepId);
            dataPacket.setTimestamp(timestamp);


            for(int j = 2;j<cs.length-1;j++){
                String sensorName = columns[j];
                Double value = Double.parseDouble(cs[j]);
                SensorData sensorData = SensorData.build();
                sensorData.setStepId(stepId);
                sensorData.setValue(value);
                sensorData.setSensorAlias(sensorName);
                sensorData.setSensorName(sensorName);
                sensorData.setTimestamp(timestamp);
                dataPacket.getSensorDataMap().put(sensorName,sensorData);
            }
            SensorData sensorData = SensorData.build();
            sensorData.setStepId(stepId);
            sensorData.setValue(stepId);
            sensorData.setSensorAlias("StepID");
//            sensorData.setSensorName("StepID");
            sensorData.setTimestamp(timestamp);
            dataPacket.getSensorDataMap().put("StepID",sensorData);

            dataFlow.getDataList().add(dataPacket);

        }
        dataFlow.setStartTime(startTime);
        return dataFlow;
    }

    public static void main(String[] args) throws IOException {
       IDataFlow dataFlow = RawDataBuilder.loadDataFlow("t.data");
        System.out.println(dataFlow);
    }
}
