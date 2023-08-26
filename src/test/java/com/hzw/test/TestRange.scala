package com.hzw.test

import scala.collection.mutable.ListBuffer

object TestRange {

  def main(args: Array[String]): Unit = {

    var passList = new ListBuffer[String] ()

    val errorCodes = "1,2,3"

    passList = errorCodes.split(",").to[ListBuffer]

    println(errorCodes)
    println(passList)


    val oocLevel = 0
    var startIndex = -3
    var endIndex = 4
    if(oocLevel > 0 && oocLevel < 4){
      startIndex = 1
      endIndex = oocLevel + 1
    }else if (oocLevel < 0 && oocLevel > -4){
      startIndex = oocLevel
      endIndex = 0
    }else{
      startIndex = 0
      endIndex = 0
    }

    for(i <- Range(startIndex,endIndex)){
      println(s"i == ${i}")
    }
  }

}
