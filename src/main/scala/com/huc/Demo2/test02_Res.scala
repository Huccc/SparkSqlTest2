package com.huc.Demo2

import com.alibaba.fastjson._
import com.huc.utils.KafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

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
object test02_Res {
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
      val value: RDD[(String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String)] = rdd.map(x => {
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

        // TODO 取出字段BillOfLadingInformation
        val BillOfLadingInformation: JSONArray = json.getJSONArray("BillOfLadingInformation")

        val Bill_arr: ArrayBuffer[(String, String, String, String, String, String, String, String)] = ArrayBuffer[(String, String, String, String, String, String, String, String)]()

        for (i <- 0 until BillOfLadingInformation.size()) {
          val BillObj: JSONObject = BillOfLadingInformation.getJSONObject(i)
          val DangerousCargoInformation: JSONArray = BillObj.getJSONArray("DangerousCargoInformation")

          var RecId3: String = BillObj.getString("RecId3")
          if (RecId3 == null) {
            RecId3 = "null"
          }
          var BlNo: String = BillObj.getString("BlNo")
          if (BlNo == null) {
            BlNo = "null"
          }

          for (i <- 0 until DangerousCargoInformation.size()) {
            val DangerousObj: JSONObject = DangerousCargoInformation.getJSONObject(i)
            val PackagesInformation: JSONArray = DangerousObj.getJSONArray("PackagesInformation")

            var RecId: String = DangerousObj.getString("RecId")
            if (RecId == null) {
              RecId = "null"
            }
            var DgrgdAuditRcpNo: String = DangerousObj.getString("DgrgdAuditRcpNo")
            if (DgrgdAuditRcpNo == null) {
              DgrgdAuditRcpNo = "null"
            }

            for (i <- 0 until PackagesInformation.size()) {
              val PackObj: JSONObject = PackagesInformation.getJSONObject(i)
              var RecId7: String = PackObj.getString("RecId")
              if (RecId7 == null) {
                RecId7 = "null"
              }
              var PackagesNo: String = PackObj.getString("PackagesNo")
              if (PackagesNo == null) {
                PackagesNo = "null"
              }

              val UnitInformation: JSONArray = DangerousObj.getJSONArray("UnitInformation")
              for (i <- 0 until UnitInformation.size()) {
                val UnitObj: JSONObject = UnitInformation.getJSONObject(i)
                var RecId8: String = UnitObj.getString("RecId")
                if (RecId8 == null) {
                  RecId8 = "null"
                }
                var TypeOfUnit: String = UnitObj.getString("TypeOfUnit")
                if (TypeOfUnit == null) {
                  TypeOfUnit = "null"
                }
                Bill_arr += ((RecId3, BlNo, RecId, DgrgdAuditRcpNo, RecId7, PackagesNo, RecId8, TypeOfUnit))
              }
            }
          }
        }

        // TODO 取出字段TailRecord
        val TailRecord: JSONObject = json.getJSONObject("TailRecord")
        var RecId4: AnyRef = TailRecord.get("RecId")
        if (RecId4 == null) {
          RecId4 = "null"
        }
        var RecTtlQty: AnyRef = TailRecord.get("RecTtlQty")
        if (RecTtlQty == null) {
          RecTtlQty = "null"
        }

        // TODO 取出字段AppeneixInformation
        val AppeneixInformation: JSONArray = json.getJSONArray("AppeneixInformation")
        val AppeneixInformation_arr: ArrayBuffer[(String, String)] = ArrayBuffer[(String, String)]()
        for (i <- 0 until AppeneixInformation.size()) {
          val obj1: JSONObject = AppeneixInformation.getJSONObject(i)
          var RecId5: String = obj1.getString("RecId")
          if (RecId5 == null) {
            RecId5 = "null"
          }
          var AppendixType: String = obj1.getString("AppendixType")
          if (AppendixType == null) {
            AppendixType = "null"
          }
          AppeneixInformation_arr += ((RecId5, AppendixType))
        }

        ((RecId2, MsgType, RecId1, VesselNo, RecId4, RecTtlQty), Bill_arr, AppeneixInformation_arr)
      })
        .flatMap(x => {
          val data_arr: ArrayBuffer[(String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String)] = ArrayBuffer[(String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String)]()
          val header: (AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef) = x._1
          val array2: Array[(String, String, String, String, String, String, String, String)] = x._2.toArray
          val array3: Array[(String, String)] = x._3.toArray
          for (elem2 <- array2) {
            for (elem3 <- array3) {
              data_arr += ((header._1.toString, header._2.toString, header._3.toString, header._4.toString, header._5.toString, header._6.toString, elem2._1, elem2._2, elem2._3, elem2._4, elem2._5, elem2._6, elem2._7, elem2._8, elem3._1, elem3._2))
            }
          }
          data_arr
        })

      val df: DataFrame = value.toDF("RecId2", "MsgType", "RecId1", "VesselNo", "RecId4", "RecTtlQty", "RecId3", "BlNo", "RecId", "DgrgdAuditRcpNo", "RecId7", "PackagesNo", "RecId8", "TypeOfUnit", "RecId5", "AppendixType")

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
