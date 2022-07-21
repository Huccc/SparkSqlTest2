package com.huc.Demo1

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.{DataFrame, SparkSession}

object test02_kakfa_sql {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName(this.getClass.getSimpleName).getOrCreate()

//    session.conf.set("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    spark.sql(
      """
        |CREATE TABLE kafka_data_exchange_kafka_topic
        |USING kafka
        |OPTIONS (
        |  kafka.bootstrap.servers '192.168.129.121:9092,192.168.129.122:9092,192.168.129.123:9092',
        |  subscribe 'mt1101_test_bill',
        |  format 'json'
        |)
        |""".stripMargin)

    val df1: DataFrame = spark.sql(
      """
        |select
        |  topic,partition,offset,timestamp,cast(value as String) as value
        |from kafka_data_exchange_kafka_topic
        |""".stripMargin)
//    df1.show(false)
    import org.apache.spark.sql.functions._

    val df2: DataFrame = df1.select(
      col("topic"),
      get_json_object(col("value"), "$.APP_NAME").alias("APP_NAME"),
      get_json_object(col("value"), "$.TABLE_NAME").alias("TABLE_NAME"),
      get_json_object(col("value"), "$.SUBSCRIBE_TYPE").alias("SUBSCRIBE_TYPE"),
      get_json_object(col("value"), "$.DATA").alias("DATA")
    )

    df2.select(
      col("topic"),
      col("APP_NAME"),
      col("TABLE_NAME"),
      col("SUBSCRIBE_TYPE"),
      get_json_object(col("DATA"), "$.VSL_IMO_NO").alias("VSL_IMO_NO"),
      get_json_object(col("DATA"), "$.VSL_NAME").alias("VSL_NAME")
    ).show(false)

    spark.close()
  }

}
