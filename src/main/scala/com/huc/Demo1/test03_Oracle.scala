package com.huc.Demo1

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

object test03_Oracle {
  def main(args: Array[String]): Unit = {
    val sparksession: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName(this.getClass.getSimpleName)
      .getOrCreate()

    println(this.getClass.getSimpleName)

    val sc: SparkContext = sparksession.sparkContext

    val props: Properties = new Properties()
    props.setProperty("user", "xqwu")
    props.setProperty("password", "easipass")

    val df: DataFrame = sparksession.read.jdbc("jdbc:oracle:thin:@192.168.129.149:1521/test12c", "DM.TRACK_BIZ_STATUS_BILL", props)

    df.createOrReplaceTempView("TRACK_BIZ_STATUS_BILL")

    val sparksql: String =
      s"""
         |select
         |  VSL_IMO_NO,VSL_NAME,BL_NO,current_timestamp
         |from
         |  TRACK_BIZ_STATUS_BILL as t
         |where
         |  t.VSL_IMO_NO <> 'N/A'
         |""".stripMargin

    val df2: DataFrame = sparksession.sql(sparksql)

    df2.show(false)

    sparksession.close()
  }

}
