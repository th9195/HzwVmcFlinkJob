package com.hzw.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.hzw.fdc.engine.api.ApiControlWindow;
import com.hzw.fdc.engine.controlwindow.ContainerWindow;
import com.hzw.fdc.engine.controlwindow.ControlWindow;
import com.hzw.fdc.engine.controlwindow.data.WindowIndicatorConfig;
import com.hzw.fdc.engine.controlwindow.data.impl.DataFlow;
import com.hzw.fdc.engine.controlwindow.data.impl.DataPacket;
import com.hzw.fdc.engine.controlwindow.data.impl.SensorData;
import com.hzw.fdc.engine.controlwindow.data.impl.WindowClipResult;
import com.hzw.fdc.scalabean.WindowData;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class FindBug {
	@Test
	public void test(){
		try {
			System.out.println(Paths.get("." ).toAbsolutePath());
			byte[] data = Files.readAllBytes(Paths.get("../datas/wd/t5_raw3.json"  ));
			String s = new String(data);
			JSONArray array = JSON.parseArray(s);
			DataFlow dataFlow = (DataFlow) ApiControlWindow.buildDataFlow();
			for(int i=0;i<array.size();i++){
				JSONObject o = array.getJSONObject(i);
				int stepId = o.getInteger("stepId");
				long timestamp = o.getLong("timestamp");
				DataPacket dp = (DataPacket) ApiControlWindow.buildDataPacket();
				dp.setStepId(stepId);
				dp.setTimestamp(timestamp);
				dp.setIndex(i);

				JSONArray sensorDataList = o.getJSONArray("sensorDataList");
				for(int j=0;j<sensorDataList.size();j++){
					JSONObject sensor = sensorDataList.getJSONObject(j);
					SensorData sd = (SensorData) ApiControlWindow.buildSensorData();
					sd.setTimestamp(timestamp);
					sd.setIndex(i);
					sd.setValue(sensor.getDoubleValue("value"));
					sd.setSensorAlias(sensor.getString("sensorAlias"));
					sd.setStepId(sensor.getIntValue("stepId"));
					dp.getSensorDataMap().put(sd.getSensorAlias(),sd );

				}
				dataFlow.getDataList().add(dp);
			}
			dataFlow.setStartTime(1650351166598l);
			dataFlow.setStopTime(1650351311093l);

			//// 数据准备结束


			try{
				ContainerWindow window = null;


				window =(ContainerWindow)ApiControlWindow.parse(ControlWindow.TYPE_StepIdBasedWindow,"S31b0S","S31e0S");;
				window.setWindowId(1l);
				window.addIndicatorConfig(new WindowIndicatorConfig(1l,1l,"PRESS_CUP_EXHAUST_CHAM"));

				window.attachDataFlow(dataFlow);

				Collection<WindowClipResult> windowClipResults =window.calculate();
				ContainerWindow finalWindow = window;

				ExecutorService es = Executors.newFixedThreadPool(10);

				for(int i=0;i<100;i++){
					int finalI = i;
					es.execute(new Runnable(){

						@Override
						public void run() {
							try{
								finalWindow.attachDataFlow(dataFlow);
								Collection<WindowClipResult> windowClipResults = finalWindow.calculate();
								System.out.println(Thread.currentThread().getId()+":"+ finalI +":"+windowClipResults.size());
							}catch (Throwable t){
								t.printStackTrace();
								System.out.println("error: "+Thread.currentThread().getId()+":"+ finalI);
							}

						}
					});
				}


				window.attachDataFlow(dataFlow);

				 windowClipResults =window.calculate();
//
//				window.attachDataFlow(dataFlow);
//
//				windowClipResults =window.calculate();

				for(WindowClipResult result : windowClipResults){
					WindowData windowData = ApiControlWindow.convertWindowClipResultToWindowData(result);
					System.out.println(windowData);
				}

				System.out.println(windowClipResults);
			}catch(Exception e){
				e.printStackTrace();
			}





			System.out.println(dataFlow);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
