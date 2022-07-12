package com.huc.Demo2

import scala.collection.mutable.ArrayBuffer

/**
 * Created with IntelliJ IDEA.
 *
 * @Project : SparkSqlTest2
 * @Package : com.huc.Demo2
 * @createTime : 2022/7/5 9:57
 * @author : huc
 * @Email : 1310259975@qq.com
 * @Description : 
 */
object ArrayBufferTest1 {
  def main(args: Array[String]): Unit = {
    println("My bikes are ")
    val bikes = ArrayBuffer("ThunderBird 350", "YRF R3")
    for (i <- 0 to bikes.length - 1)
      println(bikes(i))

    // adding a string
    bikes += "iron 883"
    println("After adding new bike to my collection")
    for (i <- 0 to bikes.length - 1)
      println(bikes(i))
  }
}
