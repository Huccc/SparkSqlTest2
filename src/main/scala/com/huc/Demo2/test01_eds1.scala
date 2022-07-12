package com.huc.Demo2

import com.huc.utils.KafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.alibaba.fastjson._
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/**
 * Created with IntelliJ IDEA.
 *
 * @Project : SparkSqlTest2
 * @Package : com.huc.Demo2
 * @createTime : 2022/7/11 11:03
 * @author : huc
 * @Email : 1310259975@qq.com
 * @Description : 
 */
object test01_eds1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("test01")

    val ssc = new StreamingContext(conf = conf, batchDuration = Seconds(5))

    val spark: SparkSession = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtil.getKafkaStream("eds_source_test", ssc)

    val lineDStream: DStream[String] = kafkaDStream.map(_.value())

    import spark.implicits._
    lineDStream.foreachRDD(rdd => {
      val value: RDD[(String, String, String, String, String, String, String, String)] = rdd.map(x => {
        val json: JSONObject = JSON.parseObject(x)
        // TODO 取出字段VesselVoyageInformation
        val VesselVoyageInformation: JSONObject = json.getJSONObject("VesselVoyageInformation")
        var RecId1: AnyRef = VesselVoyageInformation.get("RecId1")
        if (RecId1 == null) {
          RecId1 = "null"
        }
        var VesselNo: AnyRef = VesselVoyageInformation.get("VesselNo")
        if (VesselNo == null) {
          VesselNo = "null"
        }

        // TODO 取出字段HeadRecord
        val HeadRecord: JSONObject = json.getJSONObject("HeadRecord")
        var RecId2: AnyRef = HeadRecord.get("RecId2")
        if (RecId2 == null) {
          RecId2 = "null"
        }
        var MsgType: AnyRef = HeadRecord.get("MsgType")
        if (MsgType == null) {
          MsgType = "null"
        }

        // TODO 取出字段TailRecord
        val TailRecord: JSONObject = json.getJSONObject("TailRecord")
        var RecId3: AnyRef = TailRecord.get("RecId")
        if (RecId3 == null) {
          RecId3 = "null"
        }
        var RecTtlQty: AnyRef = TailRecord.get("RecTtlQty")
        if (RecTtlQty == null) {
          RecTtlQty = "null"
        }

        // TODO 取出字段BillOfLadingInformation

        // TODO 取出字段AppeneixInformation
        val AppeneixInformation: JSONArray = json.getJSONArray("AppeneixInformation")
        val AppeneixInformation_arr: ArrayBuffer[(String, String)] = ArrayBuffer[(String, String)]()
        for (i <- 0 until AppeneixInformation.size()) {
          val obj1: JSONObject = AppeneixInformation.getJSONObject(i)
          var RecId: String = obj1.getString("RecId")
          if (RecId == null) {
            RecId = "null"
          }
          var AppendixType: String = obj1.getString("AppendixType")
          if (AppendixType == null) {
            AppendixType = "null"
          }
          AppeneixInformation_arr += ((RecId, AppendixType))
        }


        ((RecId1, VesselNo, RecId2, MsgType, RecId3, RecTtlQty), AppeneixInformation_arr)
      })
        .flatMap(x => {
          val data_arr: ArrayBuffer[(String, String, String, String, String, String, String, String)] = ArrayBuffer[(String, String, String, String, String, String, String, String)]()
          val value1: (AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef) = x._1
          val array1: Array[(String, String)] = x._2.toArray
          for (elem <- array1) {
            data_arr += ((value1._1.toString, value1._2.toString, value1._3.toString, value1._4.toString, value1._5.toString, value1._6.toString,
              elem._1, elem._2))
          }
          data_arr
        })
      val df: DataFrame = value.toDF("RecId1", "VesselNo", "RecId2", "MsgType", "RecId3", "RecTtlQty", "RecId", "AppendixType")

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
