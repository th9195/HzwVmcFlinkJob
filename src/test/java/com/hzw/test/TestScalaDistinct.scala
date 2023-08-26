package com.hzw.test

object TestScalaDistinct {
  case class Person(name: String, age: Int)

  def main(args: Array[String]): Unit = {
    val list = List(Person("Alice", 20), Person("Bob", 30), Person("Alice", 20))
    val distinctList = list.distinct
    println(s"list == ${list}")
    println(s"distinctList == ${distinctList}")
  }


}
