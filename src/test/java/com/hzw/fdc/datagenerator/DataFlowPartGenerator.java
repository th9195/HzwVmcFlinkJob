package com.hzw.fdc.datagenerator;

import com.hzw.fdc.engine.controlwindow.data.defs.IDataPacket;
import com.hzw.fdc.engine.controlwindow.data.defs.ISensorData;
import com.hzw.fdc.engine.controlwindow.data.impl.DataPacket;
import com.hzw.fdc.engine.controlwindow.data.impl.SensorData;

import java.util.ArrayList;
import java.util.List;

public abstract class DataFlowPartGenerator {


     double x_start=0;
     int count=10;
     double dx = 0.1;
     int stepId = 1;
     String sensor="";
     double sigma = 2.2;

    public List<IDataPacket> generate() {
        List<IDataPacket> dataPackets = new ArrayList<>();
        for(int i=0;i<count;i++){
            ISensorData sensorData = SensorData.build();
            sensorData.setSensorName(sensor);
            sensorData.setSensorAlias(sensor);
            sensorData.setStepId(stepId);
            sensorData.setValue(f(x_start+dx*i)+makeSigma());
            IDataPacket packet = DataPacket.build();
            packet.setStepId(stepId);
            packet.getSensorDataMap().put(sensor,sensorData);
            dataPackets.add(packet);

        }
        return dataPackets;
    }

    public double makeSigma(){
        double sig = Math.pow(-1,(int)(Math.random()*2))*Math.random()*sigma;
        return sig;
    }
    public abstract double f(double x);




}
