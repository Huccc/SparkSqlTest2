package com.huc.DsDemo1

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Created with IntelliJ IDEA.
 *
 * @Project : SparkSqlTest2
 * @Package : com.huc.DsDemo1
 * @createTime : 2022/7/7 19:27
 * @author : huc
 * @Email : 1310259975@qq.com
 * @Description : 
 */
object test01 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparksql")
    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val df2: DataFrame = session.read.json("input/test2.json")


  }
}
