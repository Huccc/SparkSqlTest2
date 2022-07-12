package com.huc.KafkaSink

import java.util.Properties
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.jackson.Json

/**
 * Created with IntelliJ IDEA.
 *
 * @Project : SparkSqlTest2
 * @Package : com.huc.KafkaSink
 * @createTime : 2022/7/6 15:21
 * @author : huc
 * @Email : 1310259975@qq.com
 * @Description : 
 */
object SendKafkaDemo2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("app")
    val sc: SparkContext = new SparkContext(conf)
    val rdd: RDD[String] = sc.parallelize(Array("1", "2", "3", "4"))
    val rdd2: RDD[String] = sc.textFile("input/test2.json")
    // 广播KafkaSink
    val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
      val kafkaProducerConfig = {
        val p = new Properties()
        p.setProperty("bootstrap.servers", "192.168.129.121:9092,192.168.129.122:9092,192.168.129.123:9092") //修改为你的kafka地址
        p.setProperty("key.serializer", classOf[StringSerializer].getName)
        p.setProperty("value.serializer", classOf[StringSerializer].getName)
        p
      }
      sc.broadcast(KafkaSink[String, String](kafkaProducerConfig))
    }
    rdd2.foreach(record => {
      kafkaProducer.value.send("eds_source_test", record)
    })
  }

}
