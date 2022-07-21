package com.huc.Demo1

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.codehaus.jackson.map.deser.std.StringDeserializer

object test02_kafka {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparksql")
    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val dataFrame: DataFrame = session.read
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.129.121:9092,192.168.129.122:9092,192.168.129.123:9092")
//      .option("startingOffsets","group offsets")
//      .option("endingOffsets","latest")
      .option("subscribe", "mt1101_test_bill")
//      .option("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
//      .option("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")
      .load()

    dataFrame.createOrReplaceTempView("test")

    session.sql(
      """
        |select
        |  cast(value as String)
        |from
        |  test
        |""".stripMargin).show(false)

    session.close()
  }

}
