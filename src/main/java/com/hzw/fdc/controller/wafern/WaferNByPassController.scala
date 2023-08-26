package com.hzw.fdc.controller.wafern

import com.fasterxml.jackson.databind.JsonNode
import com.hzw.fdc.function.online.MainFabAlarm.{IndicatorAlarmProcessFunctionNew}
import com.hzw.fdc.scalabean.IndicatorResult
import com.hzw.fdc.wafern.IndependentConditionalWaferNByPassDelegator
import com.hzw.fdc.wafern.`type`.{BypassCondition, IndicatorResultBean}
import org.apache.flink.configuration.Configuration
import org.slf4j.{Logger, LoggerFactory}

import java.util

class WaferNByPassController extends Serializable {

  private val logger: Logger = LoggerFactory.getLogger(classOf[WaferNByPassController])

  val byPassControllerDelegator = new IndependentConditionalWaferNByPassDelegator()

  /**
   *  与框架结构统一，视情况而定在初始化时做必要的动作
   * @param parameters
   * @param function
   */
//  def open(parameters: Configuration, function: IndicatorAlarmProcessFunction): Unit = {
//
//  }

  def open_new(parameters: Configuration, function: IndicatorAlarmProcessFunctionNew): Unit = {

  }


  /**
   * 该方法在配置广播流收到一条数据时进行更新。目前被安排在processBroadcastElement方法的最底部被调用。
   * 该方法设计上目前的考虑是自行消化内部的异常，不得抛出和阻塞原流程。
   * @param config
   */
//   def updateConfig(config: JsonNode, caller: IndicatorAlarmProcessFunction) = {
//
//  }

  def updateConfig_new(config: JsonNode, caller: IndicatorAlarmProcessFunctionNew) = {

  }


  /**
   * 该方法在数据流收到一个indicator result对象时被调用
   * @param indicatorResult
   * @return
   */
//  def shouldByPass(indicatorResult: IndicatorResult, caller: IndicatorAlarmProcessFunction):Boolean = {
//
//    var bean = buildIndicatorResultBean(indicatorResult);
//
//    return byPassControllerDelegator.shouldByPass(Array(bean));
//  }

  def shouldByPass_new(indicatorResult: IndicatorResult, caller: IndicatorAlarmProcessFunctionNew):Boolean = {

    var bean = buildIndicatorResultBean(indicatorResult);

    return byPassControllerDelegator.shouldByPass(Array(bean));
  }


  /**
   * 重新装配controller所需要的bean
   * @param indicatorResult
   * @return
   */
  def buildIndicatorResultBean(indicatorResult: IndicatorResult): IndicatorResultBean = {
    var bean = new IndicatorResultBean();
    bean.setIndicatorId(indicatorResult.indicatorId)
    bean.setRecipeName(indicatorResult.recipeName)
    bean.setPMStatus(indicatorResult.pmStatus)
    bean.setPMTimestamp(indicatorResult.pmTimestamp)
    bean.setToolName(indicatorResult.toolName);
    bean.setChamberName(indicatorResult.chamberName);

    var productList = new util.ArrayList[String]();
    var bypassConditionList = new util.ArrayList[BypassCondition]()

    if(indicatorResult.product!=null){
      indicatorResult.product.foreach((p)=>{productList.add(p)})
    }
    bean.setProductName(productList)

    if(indicatorResult.bypassCondition.nonEmpty){
      indicatorResult.bypassCondition.get.details.foreach((bp)=>{
        var bpObj = new BypassCondition()
        bpObj.setCondition( bp.condition)
        bpObj.setRecentRuns(bp.recentRuns.toInt)
        bypassConditionList.add(bpObj)
      });
    }
    bean.setBypassCondition(bypassConditionList)
    return bean;
  }






}
