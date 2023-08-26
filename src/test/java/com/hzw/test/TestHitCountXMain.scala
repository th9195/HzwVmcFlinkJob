//package com.hzw.test
//
//import com.hzw.fdc.function.online.MainFabIndicator.IndicatorAlgorithm
//import com.hzw.fdc.scalabean.RawDataTimeValueScala
//
//import scala.collection.mutable.ListBuffer
//
//object TestHitCountXMain {
//  def main(args: Array[String]): Unit = {
//
//    test1();
//    test2();
//    test3();
//    test4();
//    test5();
//    test6();
//    test7();
//    test8();
//    test9();
//    test10();
//    test11();
//    return 0;
//  }
//
//  def test1(): Unit ={
//    var buffer = ListBuffer(
//      RawDataTimeValueScala("1","1","-1"),
//      RawDataTimeValueScala("1","2","1"),
//      RawDataTimeValueScala("1","3","-1"),
//      RawDataTimeValueScala("1","4","1"),
//      RawDataTimeValueScala("1","5","-1")
//    )
//    var count = IndicatorAlgorithm.HitCountAlgorithm(buffer,"0","x");
//    println("count: "+count+" right:"+(count=="4"))
//    return 0;
//  }
//
//  def test2(): Unit ={
//    var buffer = ListBuffer(
//      RawDataTimeValueScala("1","1","-1"),
//      RawDataTimeValueScala("1","2","-1"),
//      RawDataTimeValueScala("1","3","0"),
//      RawDataTimeValueScala("1","4","0"),
//      RawDataTimeValueScala("1","5","-1"),
//      RawDataTimeValueScala("1","6","1")
//    )
//    var count = IndicatorAlgorithm.HitCountAlgorithm(buffer,"0","x");
//    println("count: "+count+" right:"+(count=="1"))
//    return 0;
//  }
//
//  def test3(): Unit ={
//    var buffer = ListBuffer(
//      RawDataTimeValueScala("1","1","-1"),
//      RawDataTimeValueScala("1","2","-1"),
//      RawDataTimeValueScala("1","3","0"),
//      RawDataTimeValueScala("1","4","0"),
//      RawDataTimeValueScala("1","5","-1"),
//      RawDataTimeValueScala("1","6","1"),
//      RawDataTimeValueScala("1","6","1"),
//      RawDataTimeValueScala("1","6","0"),
//      RawDataTimeValueScala("1","6","-1")
//
//    )
//    var count = IndicatorAlgorithm.HitCountAlgorithm(buffer,"0","x");
//    println("count: "+count+" right:"+(count=="2"))
//    return 0;
//  }
//
//  def test4(): Unit ={
//    var buffer = ListBuffer(
//      RawDataTimeValueScala("1","1","-1")
//    )
//    var count = IndicatorAlgorithm.HitCountAlgorithm(buffer,"0","x");
//    println("count: "+count+" right:"+(count=="0"))
//    return 0;
//  }
//
//  def test5(): Unit ={
//    var buffer = ListBuffer(
//      RawDataTimeValueScala("1","1","-1"),
//      RawDataTimeValueScala("1","2","0")
//    )
//    var count = IndicatorAlgorithm.HitCountAlgorithm(buffer,"0","x");
//    println("count: "+count+" right:"+(count=="0"))
//    return 0;
//  }
//
//  def test6(): Unit ={
//    var buffer = ListBuffer(
//      RawDataTimeValueScala("1","1","1"),
//        RawDataTimeValueScala("1","2","0")
//    )
//    var count = IndicatorAlgorithm.HitCountAlgorithm(buffer,"0","x");
//    println("count: "+count+" right:"+(count=="0"))
//    return 0;
//  }
//
//  def test7(): Unit ={
//    var buffer = ListBuffer(
//      RawDataTimeValueScala("1","1","1"),
//        RawDataTimeValueScala("1","2","-1")
//    )
//    var count = IndicatorAlgorithm.HitCountAlgorithm(buffer,"0","x");
//    println("count: "+count+" right:"+(count=="1"))
//    return 0;
//  }
//
//  def test8(): Unit ={
//    var buffer = ListBuffer(
//      RawDataTimeValueScala("1","1","-1"),
//        RawDataTimeValueScala("1","2","1")
//    )
//    var count = IndicatorAlgorithm.HitCountAlgorithm(buffer,"0","x");
//    println("count: "+count+" right:"+(count=="1"))
//    return 0;
//  }
//
//  def test9(): Unit ={
//    var buffer = ListBuffer(
//      RawDataTimeValueScala("1","1","-1"),
//      RawDataTimeValueScala("1","1","-1"),
//        RawDataTimeValueScala("1","2","1")
//    )
//    var count = IndicatorAlgorithm.HitCountAlgorithm(buffer,"0","x");
//    println("count: "+count+" right:"+(count=="1"))
//    return 0;
//  }
//
//  def test10(): Unit ={
//    var buffer = ListBuffer(
//      RawDataTimeValueScala("1","1","1"),
//      RawDataTimeValueScala("1","1","1"),
//      RawDataTimeValueScala("1","2","-1")
//    )
//    var count = IndicatorAlgorithm.HitCountAlgorithm(buffer,"0","x");
//    println("count: "+count+" right:"+(count=="1"))
//    return 0;
//  }
//
//  def test11(): Unit ={
//    var buffer = ListBuffer(
//      RawDataTimeValueScala("1","1","1"),
//      RawDataTimeValueScala("1","1","-1"),
//      RawDataTimeValueScala("1","2","-1")
//    )
//    var count = IndicatorAlgorithm.HitCountAlgorithm(buffer,"0","x");
//    println("count: "+count+" right:"+(count=="1"))
//    return 0;
//  }
//
//  def test12(): Unit ={
//    var buffer = ListBuffer(
//      RawDataTimeValueScala("1","1","-1"),
//      RawDataTimeValueScala("1","1","-1"),
//      RawDataTimeValueScala("1","2","-1")
//    )
//    var count = IndicatorAlgorithm.HitCountAlgorithm(buffer,"0","x");
//    println("count: "+count+" right:"+(count=="0"))
//    return 0;
//  }
//
//  def test13(): Unit ={
//    var buffer = ListBuffer(
//      RawDataTimeValueScala("1","1","1"),
//      RawDataTimeValueScala("1","1","1"),
//      RawDataTimeValueScala("1","2","1")
//    )
//    var count = IndicatorAlgorithm.HitCountAlgorithm(buffer,"0","x");
//    println("count: "+count+" right:"+(count=="0"))
//    return 0;
//  }
//
//  def test14(): Unit ={
//    var buffer = ListBuffer(
//      RawDataTimeValueScala("1","1","0"),
//      RawDataTimeValueScala("1","1","0"),
//      RawDataTimeValueScala("1","2","0")
//    )
//    var count = IndicatorAlgorithm.HitCountAlgorithm(buffer,"0","x");
//    println("count: "+count+" right:"+(count=="0"))
//    return 0;
//  }
//
//
//
//}