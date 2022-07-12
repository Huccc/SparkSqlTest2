package com.huc.Demo1

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.huc.utils.KafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer

object test02_kafka_streaming4 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName(this.getClass.getSimpleName)

    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

    val spark: SparkSession = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtil.getKafkaStream("eds_source_test", ssc)

    val lineDStream: DStream[String] = kafkaDStream.map(_.value())
    lineDStream.print()
    import spark.implicits._

    lineDStream.foreachRDD(rdd => {
      val df: DataFrame = spark.read.json(spark.createDataset(rdd))
      df.show(false)
      df.createOrReplaceTempView("test")
      spark.sql(
        """
          |select * from test
          |""".stripMargin).show(false)
    })


    ssc.start()
    ssc.awaitTermination()
  }

}
