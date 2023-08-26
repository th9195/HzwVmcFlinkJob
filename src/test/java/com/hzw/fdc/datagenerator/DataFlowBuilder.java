package com.hzw.fdc.datagenerator;

import com.hzw.fdc.engine.controlwindow.data.defs.IDataPacket;
import com.hzw.fdc.engine.controlwindow.data.defs.ISensorData;
import com.hzw.fdc.engine.controlwindow.data.impl.DataFlow;
import com.hzw.fdc.engine.controlwindow.data.impl.DataPacket;
import com.hzw.fdc.engine.controlwindow.data.impl.SensorData;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DataFlowBuilder {

    long start = System.currentTimeMillis();
    long dt = 1000;

    List<DataFlowPartGenerator> generators = new ArrayList<>();

    public DataFlowBuilder addGenerator(DataFlowPartGenerator generator){
        this.generators.add(generator);
        return this;
    }

    public  DataFlow build(){
        DataFlow dataFlow = DataFlow.build();
        long current = start;
        for(DataFlowPartGenerator generator: generators){
            List<IDataPacket> list = generator.generate();
            for(IDataPacket dataPacket: list){
                if(dataPacket instanceof DataPacket){
                    ((DataPacket) dataPacket).setTimestamp(current);
                    Map<String, ISensorData> map = ((DataPacket) dataPacket).getSensorDataMap();
                    for(ISensorData data: map.values()){
                        if(data instanceof SensorData){
                            ((SensorData) data).setTimestamp(current);
                        }
                    }
                    dataFlow.getDataList().add(dataPacket);
                }
                current+=dt;
            }
        }
        return dataFlow;
    }

    public static DataFlowPartGenerator buildLinear(String sensor,int stepId,int count, double x, double a, double b){
        LinearDataFlowGenerator generator = new LinearDataFlowGenerator();
        generator.count = count;
        generator.sensor = sensor;
        generator.stepId = stepId;
        generator.x_start = x;
        generator.a = a;
        generator.b = b;
        return generator;
    }

    public static DataFlowPartGenerator buildLogistic(String sensor,int stepId,int count, double x){
        LogisticGenerator generator = new LogisticGenerator();
        generator.count = count;
        generator.sensor = sensor;
        generator.stepId = stepId;
        generator.x_start = x;
        return generator;
    }

    public static DataFlowPartGenerator buildLogistic(String sensor,int stepId,int count, double x, double k, double p, double r){
        LogisticGenerator generator = new LogisticGenerator();
        generator.count = count;
        generator.sensor = sensor;
        generator.stepId = stepId;
        generator.x_start = x;
        generator.k= k;
        generator.p = p;
        generator.r = r;
        return generator;
    }

    public static void main(String[] args) {

        DataFlowBuilder builder = new DataFlowBuilder();
        builder
                .addGenerator(buildLinear("s1",1,50,10,3.6,20.5))
                .addGenerator(buildLogistic("s1",2,100,20));
        DataFlow dataFlow = builder.build();
        System.out.println(dataFlow);
    }
}
