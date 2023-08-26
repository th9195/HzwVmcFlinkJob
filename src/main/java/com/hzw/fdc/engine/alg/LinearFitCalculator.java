package com.hzw.fdc.engine.alg;

import org.apache.commons.math3.stat.regression.RegressionResults;
import org.apache.commons.math3.stat.regression.SimpleRegression;

import java.util.ArrayList;
import java.util.List;

public class LinearFitCalculator {
	private double[][] dataSet = null;

	private double[][] fitDataSet = null;

	private String function = "";




	/**
	 * 修改线性拟合的x为索引而非时间，修改依据
	 * main_fab_Phase5:PHASE5-17
	 * 优化：X 轴不应该用实际process时间，idle时间过长，slope受影响较大。建议用固定的delta x.
	 * @param dataPoints
	 * @return
	 */
	public double linearFit(AlgorithmUtils.DataPoint... dataPoints){

		if(dataPoints.length == 2){
			return (dataPoints[1].value - dataPoints[0].value)/1;
		}

		int size = dataPoints.length;
		List<double[]> data = new ArrayList<>();
		for(int i = 0 ; i < size ;i++) {
			data.add(new double[]{i, dataPoints[i].value});
		}
		dataSet = data.stream().toArray(double[][]::new);
		return fitCalc();
	}


	private   Double fitCalc() {
		double[][] data = this.dataSet;
		List<double[]> fitData = new ArrayList<>();
		SimpleRegression regression = new SimpleRegression();
		regression.addData(data); // 数据集
		/*
		 * RegressionResults 中是拟合的结果
		 * 其中重要的几个参数如下：
		 *   parameters:
		 *      0: b
		 *      1: k
		 *   globalFitInfo
		 *      0: 平方误差之和, SSE
		 *      1: 平方和, SST
		 *      2: R 平方, RSQ
		 *      3: 均方误差, MSE
		 *      4: 调整后的 R 平方, adjRSQ
		 *
		 * */
		RegressionResults results = regression.regress();
		double b = results.getParameterEstimate(0);
		double k = results.getParameterEstimate(1);
		double r2 = results.getRSquared();

		// 重新计算生成拟合曲线
		for (double[] datum : data) {
			double[] xy = {datum[0], k * datum[0] + b};
			fitData.add(xy);
		}

		return k;

	}



}