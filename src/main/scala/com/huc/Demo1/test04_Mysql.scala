package com.huc.Demo1

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

object test04_Mysql {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("test04").setMaster("local[*]")
    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()

//    val df: DataFrame = session.read.format("jdbc")
//      .option("url", "jdbc:mysql://hadoop102:3306/gmall")
//      .option("driver", "com.mysql.jdbc.Driver")
//      .option("user", "root")
//      .option("password", "123456")
//      .option("dbtable", "user_info")
//      .load()

    val properties: Properties = new Properties()
    properties.put("user","root")
    properties.put("password","123456")
    val df: DataFrame = session.read.jdbc("jdbc:mysql://hadoop102:3306/gmall", "user_info", properties)

    df.createOrReplaceTempView("user")

    session.sql(
      """
        |select
        |  id,login_name
        |from
        |  user
        |""".stripMargin).show()

//    df.show(false)

    session.close()
  }

}
