package com.hzw.fdc.function.PublicFunction.limitCondition

import com.hzw.fdc.scalabean.{AlarmRuleConfig, IndicatorResult}
import org.apache.commons.lang3.StringUtils
import org.slf4j.{Logger, LoggerFactory}

import java.lang.reflect.Field
import scala.collection.mutable.ListBuffer
import scala.math.pow

object MatchingLimitConditionFunctions {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
   * 生成实体类
   *
   * @param conf
   * @return
   */
  def produce(conf: AlarmRuleConfig): ConditionEntity = {
    new ConditionEntity(conf.toolName.getOrElse(""), conf.chamberName.getOrElse(""),
      conf.recipeName.getOrElse(""), conf.productName.getOrElse(""), conf.stage.getOrElse(""))
  }

  /**
   * 生成实体类
   *
   * @param ir
   * @return
   */
  def produce(ir: IndicatorResult): Array[ConditionEntity] = {
    if (ir.product == null || ir.stage == null || ir.product.size == 0 || ir.stage.size == 0) {
      logger.error(s"IndicatorResult product or stage is null:${ir.toString}")
      return Array(new ConditionEntity(ir.toolName, ir.chamberName, ir.recipeName, "", ""))
    }
    if (ir.product.size == 1 && ir.stage.size == 1) {
      Array(new ConditionEntity(ir.toolName, ir.chamberName, ir.recipeName,
        ir.product.head, ir.stage.head))
    } else {
      val entities = new Array[ConditionEntity](ir.product.size * ir.stage.size)
      var i = 0
      for (p <- ir.product) {
        for (s <- ir.stage) {
          entities(i) = new ConditionEntity(ir.toolName, ir.chamberName, ir.recipeName, p, s)
          i += 1
        }
      }
      entities
    }
  }


  /**
   * 生成ConditionEntity实体类
   *
   * @param ir
   * @return
   */
  def produceConditionEntity(ir: IndicatorResult): List[ConditionEntity] = {
    if (ir.product == null || ir.stage == null || ir.product.size == 0 || ir.stage.size == 0) {
      logger.error(s"IndicatorResult product or stage is null:${ir.toString}")
      return List(new ConditionEntity(ir.toolName, ir.chamberName, ir.recipeName, "", ""))
    }

    val conditionEntityList = ListBuffer[ConditionEntity]()
    ir.product.map(p => {
      ir.stage.map(s => {
        val conditionEntity = new ConditionEntity(ir.toolName, ir.chamberName, ir.recipeName, p, s)
        conditionEntityList += conditionEntity
      })
    })
    conditionEntityList.toList
  }

  val declaredFields = classOf[ConditionEntity].getDeclaredFields.filter(_.isAnnotationPresent(classOf[FieldOrder]))

  val fields = new Array[Field](declaredFields.size)

  declaredFields.foreach(field => {
      val num = field.getAnnotation(classOf[FieldOrder]).value()
      fields(num) = field
    })

  /**
   * 匹配条件
   *
   * @param condition
   * @param data
   * @return
   */
  def satisfied(condition: ConditionEntity, data: ConditionEntity): Int = {
    try {
      var value=0
      for (i <- 0 until fields.size reverse) {

        val field: Field = fields(i)

        val conditionValue = field.get(condition).asInstanceOf[String]
        val dataValue = field.get(data).asInstanceOf[String]

        if (StringUtils.isNotBlank(conditionValue) ) {

          if (conditionValue.split(",").contains(dataValue))
            value+=pow(2.toDouble,i.toDouble).toInt
          else
            return -1
        }
      }
      value
    } catch {
      case e: Exception => e.printStackTrace()
        0
    }
  }
}
