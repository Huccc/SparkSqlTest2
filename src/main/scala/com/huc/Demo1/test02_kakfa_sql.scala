package com.huc.Demo1

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object test02_kakfa_sql {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder().master("local[*]").appName(this.getClass.getSimpleName).getOrCreate()

    session.sql(
      """
        |CREATE TABLE kafka_data_exchange_kafka_topic
        |USING kafka
        |OPTIONS (
        |  kafka.bootstrap.servers '192.168.129.121:9092,192.168.129.122:9092,192.168.129.123:9092',
        |  subscribe 'mt1101_test_bill',
        |  startingOffsets 'earliest',
        |  endingOffsets 'latest',
        |  failOnDataLoss 'false'
        |)
        |""".stripMargin)

    session.sql(
      """
        |select
        |  *
        |from kafka_data_exchange_kafka_topic
        |""".stripMargin).show(false)

    session.close()
  }

}
