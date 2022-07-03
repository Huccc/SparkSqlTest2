package com.huc.Demo1

import com.huc.utils.KafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.alibaba.fastjson.JSON
import com.google.gson.Gson
import io.netty.handler.codec.spdy.SpdyDataFrame
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.parsing.json.JSON

object test02_kafka_streaming {

  def handleMessagesCaseClass(jsonStr: String): KafkaMessage = {
    val gson: Gson = new Gson()
    gson.fromJson(jsonStr,classOf[KafkaMessage])
  }

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("test02")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtil.getKafkaStream("eds_source_test", ssc)

    //    val value: DStream[String] = kafkaDStream.mapPartitions(partition => {
    //      partition.map(record => {
    ////        JSON.parseObject(record.value(),SpdyData)
    //      })
    //    })

//    val value: DStream[String] = kafkaDStream.mapPartitions(partition => {
//      partition.map(record => {
//        JSON.toJSONString(record.value())
//      })
//    })

    kafkaDStream.map(record=>handleMessagesCaseClass(record.value()))
      .foreachRDD(rdd=>{
        val spark: SparkSession = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
        val df: DataFrame = spark.createDataFrame(rdd)
        df.show()
      })
    //    value.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
