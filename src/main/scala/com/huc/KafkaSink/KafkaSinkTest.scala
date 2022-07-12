package com.huc.KafkaSink

import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.util.Properties

/**
 * Created with IntelliJ IDEA.
 *
 * @Project : SparkSqlTest2
 * @Package : com.huc.KafkaSink
 * @createTime : 2022/7/6 14:44
 * @author : huc
 * @Email : 1310259975@qq.com
 * @Description : 
 */
object KafkaSinkTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(this.getClass.getSimpleName)
      .setMaster("local[*]")

    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    val df: DataFrame = spark.read.json("input/test3.json")


    val ssc = new StreamingContext(conf, Seconds(5))

    val config = new Config

    val KafkaProducer: Broadcast[KafkaSink[String, String]] = {
      val kafkaProducerConfig = {
        val props = new Properties()
        props.setProperty("bootstrap.servers", config.brokers)
        props.setProperty("key.serializer", classOf[StringSerializer].getName)
        props.setProperty("value.serializer", classOf[StringSerializer].getName)
        props
      }
      ssc.sparkContext.broadcast(KafkaSink[String, String](kafkaProducerConfig))
    }

    df.show()


    ssc.start()
    ssc.awaitTermination()

  }
}

class Config {
  val outTopic: String = "eds_test1"
  val brokers: String = "192.168.129.121:9092,192.168.129.122:9092,192.168.129.123:9092"
}
