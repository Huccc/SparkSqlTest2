package com.huc.utils

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import java.util.Properties


object MyKafkaUtil {
  private val properties: Properties = PropertiesUtil.load("config.properties")

  val broker_list: String = properties.getProperty("kafka.broker.list")

  val kafkaParam = Map(
    "bootstrap.servers" -> "192.168.129.121:9092,192.168.129.122:9092,192.168.129.123:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "EDS.Project",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (true: java.lang.Boolean)
  )

  def getKafkaStream(topic: String, ssc: StreamingContext): InputDStream[ConsumerRecord[String, String]] = {

    val dStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam))

    dStream
  }
}
