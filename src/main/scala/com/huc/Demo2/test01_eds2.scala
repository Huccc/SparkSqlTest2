//package com.huc.Demo2
//
//import com.alibaba.fastjson._
//import com.huc.utils.KafkaUtil
//import org.apache.kafka.clients.consumer.ConsumerRecord
//import org.apache.spark.SparkConf
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.{DataFrame, SparkSession}
//import org.apache.spark.streaming.dstream.{DStream, InputDStream}
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//
//import scala.collection.mutable.ArrayBuffer
//
///**
// * Created with IntelliJ IDEA.
// *
// * @Project : SparkSqlTest2
// * @Package : com.huc.Demo2
// * @createTime : 2022/7/11 11:03
// * @author : huc
// * @Email : 1310259975@qq.com
// * @Description :
// */
//object test01_eds2 {
//  def main(args: Array[String]): Unit = {
//    val conf: SparkConf = new SparkConf()
//      .setMaster("local[*]")
//      .setAppName("test01")
//
//    val ssc = new StreamingContext(conf = conf, batchDuration = Seconds(5))
//
//    val spark: SparkSession = SparkSession.builder()
//      .config(conf)
//      .getOrCreate()
//
//    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtil.getKafkaStream("eds_source_test", ssc)
//
//    val lineDStream: DStream[String] = kafkaDStream.map(_.value())
//
//    import spark.implicits._
//    lineDStream.foreachRDD(rdd => {
//      val value: RDD[(String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String)] = rdd.map(x => {
//        val json: JSONObject = JSON.parseObject(x)
//        // TODO 1 取出字段VesselVoyageInformation
//        val VesselVoyageInformation: JSONObject = json.getJSONObject("VesselVoyageInformation")
//        var RecId1: AnyRef = VesselVoyageInformation.get("RecId1")
//        if (RecId1 == null) {
//          RecId1 = "null"
//        }
//        var VesselNo: AnyRef = VesselVoyageInformation.get("VesselNo")
//        if (VesselNo == null) {
//          VesselNo = "null"
//        }
//
//        // TODO 2 取出字段HeadRecord
//        val HeadRecord: JSONObject = json.getJSONObject("HeadRecord")
//        var RecId2: AnyRef = HeadRecord.get("RecId2")
//        if (RecId2 == null) {
//          RecId2 = "null"
//        }
//        var MsgType: AnyRef = HeadRecord.get("MsgType")
//        if (MsgType == null) {
//          MsgType = "null"
//        }
//
//        // TODO 3 取出字段TailRecord
//        val TailRecord: JSONObject = json.getJSONObject("TailRecord")
//        var RecId3: AnyRef = TailRecord.get("RecId")
//        if (RecId3 == null) {
//          RecId3 = "null"
//        }
//        var RecTtlQty: AnyRef = TailRecord.get("RecTtlQty")
//        if (RecTtlQty == null) {
//          RecTtlQty = "null"
//        }
//
//        // TODO 4 取出字段BillOfLadingInformation
//        val BillOfLadingInformation: JSONArray = json.getJSONArray("BillOfLadingInformation")
//        val BillOfLadingInformation_arr: ArrayBuffer[(String, String, ArrayBuffer[(String, String, ArrayBuffer[(String, String)], ArrayBuffer[(String, String)])])] = ArrayBuffer[(String, String, ArrayBuffer[(String, String, ArrayBuffer[(String, String)], ArrayBuffer[(String, String)])])]()
//        for (i <- 0 until BillOfLadingInformation.size()) {
//          val BillObj: JSONObject = BillOfLadingInformation.getJSONObject(i)
//
//          var RecId4: String = BillObj.getString("RecId3")
//          if (RecId4 == null) {
//            RecId4 = "null"
//          }
//          var BlNo: AnyRef = TailRecord.get("BlNo")
//          if (BlNo == null) {
//            BlNo = "null"
//          }
//
//          val DangerousCargoInformation: JSONArray = BillObj.getJSONArray("DangerousCargoInformation")
//          val DangerousCargoInformation_arr: ArrayBuffer[(String, String, ArrayBuffer[(String, String)], ArrayBuffer[(String, String)])] = ArrayBuffer[(String, String, ArrayBuffer[(String, String)], ArrayBuffer[(String, String)])]()
//          for (i <- 0 until DangerousCargoInformation.size()) {
//            val DanObj: JSONObject = DangerousCargoInformation.getJSONObject(i)
//
//            var RecId5: String = DanObj.getString("RecId")
//            if (RecId5 == null) {
//              RecId5 = "null"
//            }
//            var DgrgdAuditRcpNo: String = DanObj.getString("DgrgdAuditRcpNo")
//            if (DgrgdAuditRcpNo == null) {
//              DgrgdAuditRcpNo = "null"
//            }
//
//            val PackagesInformation: JSONArray = DanObj.getJSONArray("PackagesInformation")
//            // todo 数组PackagesInformation
//            val PackagesInformation_arr: ArrayBuffer[(String, String)] = ArrayBuffer[(String, String)]()
//            for (i <- 0 until PackagesInformation.size()) {
//              val PackObj: JSONObject = PackagesInformation.getJSONObject(i)
//              var RecId6: String = PackObj.getString("RecId")
//              if (RecId6 == null) {
//                RecId6 = "null"
//              }
//              var PackagesNo: String = PackObj.getString("PackagesNo")
//              if (PackagesNo == null) {
//                PackagesNo = "null"
//              }
//              PackagesInformation_arr += ((RecId6, PackagesNo))
//            }
//
//            val UnitInformation: JSONArray = DanObj.getJSONArray("UnitInformation")
//            // todo 数组UnitInfomation
//            val UnitInformation_arr: ArrayBuffer[(String, String)] = ArrayBuffer[(String, String)]()
//            for (i <- 0 until UnitInformation.size()) {
//              val UnitObj: JSONObject = UnitInformation.getJSONObject(i)
//              var RecId7: String = UnitObj.getString("RecId")
//              if (RecId7 == null) {
//                RecId7 = "null"
//              }
//              var TypeOfUnit: String = UnitObj.getString("TypeOfUnit")
//              if (TypeOfUnit == null) {
//                TypeOfUnit = "null"
//              }
//              UnitInformation_arr += ((RecId7, TypeOfUnit))
//            }
//            DangerousCargoInformation_arr += ((RecId5, DgrgdAuditRcpNo, PackagesInformation, UnitInformation_arr))
//          }
//          BillOfLadingInformation_arr += ((RecId4, BlNo, DangerousCargoInformation_arr))
//        }
//
//        // TODO 5 取出字段AppeneixInformation
//        val AppeneixInformation: JSONArray = json.getJSONArray("AppeneixInformation")
//        val AppeneixInformation_arr: ArrayBuffer[(String, String)] = ArrayBuffer[(String, String)]()
//        for (i <- 0 until AppeneixInformation.size()) {
//          val obj1: JSONObject = AppeneixInformation.getJSONObject(i)
//          var RecId: String = obj1.getString("RecId")
//          if (RecId == null) {
//            RecId = "null"
//          }
//          var AppendixType: String = obj1.getString("AppendixType")
//          if (AppendixType == null) {
//            AppendixType = "null"
//          }
//          AppeneixInformation_arr += ((RecId, AppendixType))
//        }
//
//
//        ((RecId1, VesselNo, RecId2, MsgType, RecId3, RecTtlQty), AppeneixInformation_arr, BillOfLadingInformation_arr)
//      })
//        .flatMap(x => {
//          val res_arr: ArrayBuffer[(String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String)] = ArrayBuffer[(String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String)]()
//
//          val value1: (AnyRef, AnyRef, AnyRef, AnyRef, AnyRef, AnyRef) = x._1
//          val array2: Array[(String, String)] = x._2.toArray
//          val array3: Array[(String, String, ArrayBuffer[(String, String, ArrayBuffer[(String, String)], ArrayBuffer[(String, String)])])] = x._3.toArray
//
//          for (elem <- array2) {
//            res_arr += ((value1._1.toString, value1._2.toString, value1._3.toString, value1._4.toString, value1._5.toString, value1._6.toString,
//              elem._1, elem._2))
//          }
//
//          for (elem1 <- array3) {
//            for (elem2 <- elem1._3) {
//              for (elem3 <- elem2._3) {
//                res_arr += ((elem3._1, elem3._2))
//              }
//              for (elem4 <- elem2._4) {
//                res_arr += ((elem4._1, elem4._2))
//              }
//              res_arr += ((elem2._1, elem2._2))
//            }
//            res_arr += ((elem1._1, elem1._2))
//          }
//
//          res_arr
//        })
//      val df: DataFrame = value.toDF("RecId1", "VesselNo", "RecId2", "MsgType", "RecId3", "RecTtlQty", "RecId", "AppendixType","RecId6","PackagesNo","RecId7","TypeOfUnit","RecId5","DgrgdAuditRcpNo","RecId4","BlNo")
//
//      df.createOrReplaceTempView("test")
//
//      spark.sql(
//        """
//          |select * from test
//          |""".stripMargin).show(false)
//    })
//
//    ssc.start()
//    ssc.awaitTermination()
//  }
//}
