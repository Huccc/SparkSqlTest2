package com.huc.KafkaSink.demo1

import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.json4s.jackson.Serialization
import org.json4s.{Formats, NoTypeHints}

import java.util.Properties

/**
 * Created with IntelliJ IDEA.
 *
 * @Project : SparkSqlTest2
 * @Package : com.huc.KafkaSink
 * @createTime : 2022/7/6 15:08
 * @author : huc
 * @Email : 1310259975@qq.com
 * @Description :
 */
object SendKafkaDemo {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("SendKafkaDemo")
      .setMaster("local[*]")

    //    println(this.getClass.getSimpleName)

    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val sc = spark.sparkContext

    val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
      val kafkaProducerConfig = {
        val p = new Properties()
        p.setProperty("bootstrap.servers", "192.168.129.121:9092,192.168.129.122:9092,192.168.129.123:9092")
        p.setProperty("key.serializer", classOf[StringSerializer].getName)
        p.setProperty("value.serializer", classOf[StringSerializer].getName)
        p
      }
      spark.sparkContext.broadcast(KafkaSink[String, String](kafkaProducerConfig))
    }

    val action_topic = "eds_test1"
    val jsonPath = "input/user.json"
    val df = spark.read.format("json").json(jsonPath).toDF("age", "name")
    df.foreach(row => {
      val age = row.getString(row.fieldIndex("age"))
      val name = row.getString(row.fieldIndex("name"))
      val columnMap = Map(
        "age" -> age,
        "name" -> name
      )
      implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)
      val formatStr = Serialization.write(columnMap)

      println(s"推送完成标志：$action_topic  " + formatStr)
      kafkaProducer.value.send(action_topic, formatStr)
      println(1)
    })
    sc.stop()
    spark.stop()
  }
}
