package com.huc.SparkSql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

object test01_df_json_sim {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparksql")
    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val df2: DataFrame = session.read.json("input/test2.json")

    df2.createOrReplaceTempView("test2")

    session.sql(
      """
        |select
        |  BillOfLadingInformation
        |from
        |  test2
        |""".stripMargin).show(false)


    session.close()
  }

}
