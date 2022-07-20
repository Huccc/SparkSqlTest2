package com.huc.Demo3

/**
 * Created with IntelliJ IDEA.
 *
 * @Project : SparkSqlTest2
 * @Package : com.huc.Demo3
 * @createTime : 2022/7/6 9:51
 * @author : huc
 * @Email : 1310259975@qq.com
 * @Description : 
 */
object test01 {
  def main(args: Array[String]): Unit = {
    var a: Int = 1
    for (i <- 0 until 10) {
      println(i)

    }

    for (a <- 0 to 10) {
      println("a=: " + a)
    }
  }
}
