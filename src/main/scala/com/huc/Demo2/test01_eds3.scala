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
object test01_eds3 {
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
      val value: RDD[(String, String, String, String, String, String, String, String, String, String)] = rdd.map(x => {
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

        // TODO 取出字段BillOfLadingInformation
        val BillOfLadingInformation: JSONArray = json.getJSONArray("BillOfLadingInformation")

        val Bill_arr: ArrayBuffer[(String, String, String, String, String, String, String, String)] = ArrayBuffer[(String, String, String, String, String, String, String, String)]()

        for (i <- 0 until BillOfLadingInformation.size()) {
          val BillObj: JSONObject = BillOfLadingInformation.getJSONObject(i)
          val DangerousCargoInformation: JSONArray = BillObj.getJSONArray("DangerousCargoInformation")

          val RecId3: String = BillObj.getString("RecId3")
          val BlNo: String = BillObj.getString("BlNo")

          for (i <- 0 until DangerousCargoInformation.size()) {
            val DangerousObj: JSONObject = DangerousCargoInformation.getJSONObject(i)
            val PackagesInformation: JSONArray = DangerousObj.getJSONArray("PackagesInformation")

            val RecId: String = DangerousObj.getString("RecId")
            val DgrgdAuditRcpNo: String = DangerousObj.getString("DgrgdAuditRcpNo")

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

        ((RecId1, VesselNo), Bill_arr)
      })
        .flatMap(x => {
          val data_arr: ArrayBuffer[(String, String, String, String, String, String, String, String, String, String)] = ArrayBuffer[(String, String, String, String, String, String, String, String, String, String)]()
          val header: (AnyRef, AnyRef) = x._1
          val array: Array[(String, String, String, String, String, String, String, String)] = x._2.toArray
          for (elem <- array) {
            data_arr += ((header._1.toString, header._2.toString, elem._1, elem._2, elem._3, elem._4, elem._5, elem._6, elem._7, elem._8))
          }
          data_arr
        })

      val df: DataFrame = value.toDF("RecId1", "VesselNo", "RecId3", "BlNo", "RecId", "DgrgdAuditRcpNo", "RecId7", "PackagesNo", "RecId8", "TypeOfUnit")

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
