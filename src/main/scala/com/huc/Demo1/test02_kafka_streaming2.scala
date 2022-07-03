package com.huc.Demo1

import com.huc.utils.KafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}

import scala.collection.mutable.ArrayBuffer

object test02_kafka_streaming2 {
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
    val data: Unit = lineDStream.foreachRDD(rdd => {
      val df: DataFrame = rdd.map(x => {
        val json: JSONObject = JSON.parseObject(x)
        val header: JSONObject = json.getJSONObject("header")
        var appName: AnyRef = header.get("appName")
        if (appName == null) {
          appName = "null"
        }
        var ip = header.get("ip")
        if (ip == null) {
          ip = "null"
        }
        var sysTime = header.get("sysTime")
        if (sysTime == null) {
          sysTime = "null"
        }
        var userAgent = header.get("userAgent")
        if (userAgent == null) {
          userAgent = "null"
        }
        var traceId = header.get("traceId")
        if (traceId == null) {
          traceId = "null"
        }
        var deviceType = header.get("deviceType")
        if (deviceType == null) {
          deviceType = "null"
        }
        var version = header.get("version")
        if (version == null) {
          version = "null"
        }

        val body: JSONArray = json.getJSONArray("body")

        val body_arr: ArrayBuffer[(String, String, String, String, String, String, String, String, String)] = ArrayBuffer[(String, String, String, String, String, String, String, String, String)]()

        for (i <- 0 until body.size()) {
          val obj: JSONObject = body.getJSONObject(i)
          var txnType = obj.getString("txnType")
          if (txnType == null) {
            txnType = "null"
          }
          var txnCode = obj.getString("txnCode")
          if (txnCode == null) {
            txnCode = "null"
          }
          var txnSubCode = obj.getString("txnSubCode")
          if (txnSubCode == null) {
            txnSubCode = "null"
          }
          var refCode = obj.getString("refCode")
          if (refCode == null) {
            refCode = "null"
          }
          var event = obj.getString("event")
          if (event == null) {
            event = "null"
          }
          var userId = obj.getString("userId")
          if (userId == null) {
            userId = "null"
          }
          var extend = obj.getString("extend")
          if (extend == null) {
            extend = "null"
          }
          var subEvent = obj.getString("event_track")
          if (subEvent == null) {
            subEvent = "null"
          }
          var txnValue = obj.getString("txnValue")
          if (txnValue == null) {
            txnValue = "null"
          }

          body_arr += ((txnType, txnCode, txnSubCode, refCode, txnValue, event, subEvent, userId, extend))
        }
        ((traceId, appName, deviceType, version, ip, sysTime, userAgent), body_arr)
      })
        .flatMap(x => {
          val data_arr: ArrayBuffer[(String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String)] = ArrayBuffer[(String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String)]()
          val header: (AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef) = x._1
          val body_arr: Array[(String, String, String, String, String, String, String, String, String)] = x._2.toArray
          for (elem <- body_arr) {
            data_arr += ((header._1.toString, header._2.toString, header._3.toString, header._4.toString, header._5.toString, header._6.toString, header._7.toString,
              elem._1, elem._2, elem._3, elem._4, elem._5, elem._6, elem._7, elem._8, elem._9))
          }
          data_arr
        }).toDF("traceId", "appName", "deviceType", "version", "ip", "sysTime", "userAgent", "txnType", "txnCode", "txnSubCode", "refCode", "txnValue", "event", "subEvent", "userId", "extend")

      df.createOrReplaceTempView("test")

      spark.sql(
        """
          |select * from test
          |""".stripMargin).show()

    })

    ssc.start()
    ssc.awaitTermination()
  }

}
