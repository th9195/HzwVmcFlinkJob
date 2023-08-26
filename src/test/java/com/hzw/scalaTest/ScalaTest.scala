package com.hzw.scalaTest

import com.hzw.fdc.json.JsonUtil
import com.hzw.fdc.json.JsonUtil.toJson

import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ListBuffer

/**
 * ScalaTest
 *
 * @desc:
 * @author tobytang
 * @date 2022/8/22 15:41
 * @since 1.0.0
 * @update 2022/8/22 15:41
 * */
object ScalaTest {

  def main(args: Array[String]): Unit = {


    val listBuffer = ListBuffer(1, 2, 3)
    val list = List(4, 5, 6)

    listBuffer ++= list

    println(listBuffer)


  }


  private def testParitionId() = {
    val partitionMap = TrieMap[String, ListBuffer[String]]()


    for (i <- 1.to(500)) {
      var toolName = "WTOOL"
      toolName = toolName + i
      val hashCodeValue = toolName.hashCode.abs
      println(s"toolName == ${toolName} ; hashCode == ${hashCodeValue} ; ")
      val partitionId = "partition_" + hashCodeValue % 50

      val sensorList: ListBuffer[String] = partitionMap.getOrElse(partitionId, ListBuffer[String]())
      sensorList.append(toolName)
      partitionMap.put(partitionId, sensorList)
    }

    println(s"size == ${partitionMap.size} \npartitionMap == ${partitionMap}")
  }

  private def test02 = {
    val l1 = List("1", "2", "3")
    val l2 = List("3", "4", "5")
    val l3 = List("4", "5", "6", "aa")

    val l4 = l1 ++ l2 ++ l3
    val l5 = l4.distinct

    println(s"l1 == ${l1}")
    println(s"l2 == ${l2}")
    println(s"l3 == ${l3}")
    println(s"l4 == ${l4}")
    println(s"l4 distinct l5 == ${l5}")
  }

  private def test01 = {
    val result1 = dvi(1.0, 5)

    result1 match {
      case Some(x) => println(x)
      case None => println("除零异常")
    }
  }

  def dvi(a:Double, b:Double):Option[Double] = {
    if(b != 0) {
      Some(a / b)
    }
    else {
      None
    }
  }
}
