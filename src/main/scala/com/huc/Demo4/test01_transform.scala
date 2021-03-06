package com.huc.Demo4

import com.alibaba.fastjson._
import com.huc.KafkaSink.demo1.KafkaSink
import com.huc.utils.{KafkaSink, KafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.util
import java.util.Properties
import scala.collection.mutable.ArrayBuffer

/**
 * Created with IntelliJ IDEA.
 *
 * @Project : SparkSqlTest2
 * @Package : com.huc.Demo4
 * @createTime : 2022/7/18 14:38
 * @author : huc
 * @Email : 1310259975@qq.com
 * @Description : 
 */
object test01_transform {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("test01")

    val ssc = new StreamingContext(conf, Seconds(5))

    val spark: SparkSession = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtil.getKafkaStream("test_eds1", ssc)

    val value: DStream[(String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String)] = kafkaDStream
      .filter(line => {
        val lineDStream: String = line.value()
        val json: JSONObject = JSON.parseObject(lineDStream)

        json.getString("bizId") == "EDSCLR3" && json.getString("msgType") == "message_data"
      })
      .map(line => {
        val lineDStream: String = line.value()
        val json: JSONObject = JSON.parseObject(lineDStream)

        val msgId: String = json.getString("msgId")
        val bizId: String = json.getString("bizId")
        val msgType: String = json.getString("msgType")
        val bizUniqueId: String = json.getString("bizUniqueId")
        val destination: String = json.getString("destination")
        val extraInfo: JSONObject = json.getJSONObject("extraInfo")
        val parseData_arr: JSONArray = json.getJSONArray("parseData")

        // TODO ??????ArrayBuffer???????????????
        val parseDataRes: ArrayBuffer[(String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String
          )] = ArrayBuffer[(String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String)]()

        for (i <- 0 until parseData_arr.size()) {
          val parseData: JSONObject = parseData_arr.getJSONObject(i)
          val HeadRecord: JSONObject = parseData.getJSONObject("HeadRecord")
          //?????????????????????
          val FunctionCode: String = HeadRecord.getString("FileFun") //????????????
          val SenderCode: String = HeadRecord.getString("SenderCode") //???????????????
          val ReceiverCode: String = HeadRecord.getString("RecipientCode") //???????????????
          val FileCreateTime: String = HeadRecord.getString("FileCreateTime") //??????????????????
          val MessageType: String = HeadRecord.getString("MsgType") //????????????
          val Description: String = HeadRecord.getString("FileDesp") //????????????
          //?????????

          // TODO ?????????????????? IMO
          val VesselAndVoyageInformation: JSONObject = parseData.getJSONObject("VesselAndVoyageInformation")
          // todo ??????
          var vslName: String = VesselAndVoyageInformation.getString("VslName")
          if (vslName == null) {
            vslName = "null"
          }
          // todo ??????
          val voyage: String = VesselAndVoyageInformation.getString("Voyage")
          // todo IMO
          val VslImoNo: String = VesselAndVoyageInformation.getString("VslImoNo")

          // TODO ???????????????
          val BillOfLadingInformation_arr: JSONArray = parseData.getJSONArray("BillOfLadingInformation")
          for (i <- 0 until BillOfLadingInformation_arr.size()) {
            val BillOfLadingInformation: JSONObject = BillOfLadingInformation_arr.getJSONObject(i)
            // todo ?????????
            val billNo: String = BillOfLadingInformation.getString("VslVoyageBlNo")

            // TODO ???????????????????????????
            val DangerousCargoInformation_arr: JSONArray = BillOfLadingInformation.getJSONArray("DangerousCargoInformation")
            for (i <- 0 until DangerousCargoInformation_arr.size()) {
              val DangerousCargoInformation: JSONObject = DangerousCargoInformation_arr.getJSONObject(i)
              // todo ?????????????????????????????????
              val vesselDeclarationPreNo: String = DangerousCargoInformation.getString("MsaDeclAudtNo")

              // TODO ????????????
              val UnitInformation_arr: JSONArray = DangerousCargoInformation.getJSONArray("UnitInformation")
              for (i <- 0 until UnitInformation_arr.size()) {
                val UnitInformation: JSONObject = UnitInformation_arr.getJSONObject(i)
                // todo ??????
                val ctnNo: String = UnitInformation.getString("UnitIdNo")
                // todo ???UnitType???2.1??????ctnNo?????????
                val UnitType: String = UnitInformation.getString("UnitType")
                // todo ??????????????????????????? CertCtnrztnNo
                val CertCtnrztnNo: String = UnitInformation.getString("CertCtnrztnNo")
                // todo ???????????????
                val CtnrGrossWt: String = UnitInformation.getString("CtnrGrossWt")
                // todo ?????????????????????
                val PkgQtyInCtnr: String = UnitInformation.getString("PkgQtyInCtnr")
                // todo ??????????????????????????????
                val CtnrSizeType: String = UnitInformation.getString("CtnrSizeType")

                parseDataRes += ((FunctionCode, SenderCode, ReceiverCode, FileCreateTime, MessageType, Description, vslName, voyage, VslImoNo, billNo, vesselDeclarationPreNo, ctnNo, UnitType, CertCtnrztnNo, CtnrGrossWt, PkgQtyInCtnr, CtnrSizeType))
              }
            }
          }
        }
        ((msgId, bizId, msgType, bizUniqueId, destination, extraInfo), parseDataRes)
      })
      .flatMap(x => {
        val data_arr: ArrayBuffer[(String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String)] = ArrayBuffer[(String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String)]()
        val header: (String, String, String, String, String, JSONObject) = x._1
        val res_array: Array[(String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String, String)] = x._2.toArray
        for (elem <- res_array) {
          data_arr += ((header._1, header._2, header._3, header._4, header._5, elem._1, elem._2, elem._3, elem._4, elem._5, elem._6, elem._7, elem._8, elem._9, elem._10, elem._11, elem._12, elem._13, elem._14, elem._15, elem._16, elem._17))
        }
        data_arr
      })

    val props = new Properties()
    props.setProperty("user", "xqwu")
    props.setProperty("password", "easipass")
    props.setProperty("driver", "oracle.jdbc.driver.OracleDriver")

    val dfOraDanack: DataFrame = spark.read.jdbc("jdbc:oracle:thin:@192.168.129.149:1521/test12c", "ODS.DANACK_RECEIPT", props)
    val dfOraCopace: DataFrame = spark.read.jdbc("jdbc:oracle:thin:@192.168.129.149:1521/test12c", "ODS.COPACE", props)
    val dfOraCbcs: DataFrame = spark.read.jdbc("jdbc:oracle:thin:@192.168.129.149:1521/test12c", "ODS.CL_BIZ_CERT_SCHEDULE", props)

    dfOraDanack.createOrReplaceTempView("DANACK_RECEIPT")
    dfOraCopace.createOrReplaceTempView("COPACE")
    dfOraCbcs.createOrReplaceTempView("CL_BIZ_CERT_SCHEDULE")

    spark.sql(
      """
        |select * from CL_BIZ_CERT_SCHEDULE
        |""".stripMargin)

    import spark.implicits._

    val value1: DStream[Row] = value.transform(rdd => {
      val dfKafka: DataFrame = rdd.toDF("msgId", "bizId", "msgType", "bizUniqueId", "destination", "FunctionCode", "SenderCode", "ReceiverCode", "FileCreateTime", "MessageType", "Description", "vslName", "voyage", "VslImoNo", "billNo", "vesselDeclarationPreNo", "ctnNo", "UnitType", "CertCtnrztnNo", "CtnrGrossWt", "PkgQtyInCtnr", "CtnrSizeType")

      dfKafka.createOrReplaceTempView("dfKafka")

      // TODO ?????????  ???????????????????????????
      spark.sql(
        """
          |select
          |  dfKafka.*,if(COPACE.FILE_NAME <> '','Y','N') as res_1
          |from dfKafka left join COPACE on dfKafka.CertCtnrztnNo=COPACE.CTNR_PACKING_CERT_NO and dfKafka.ctnNo=COPACE.CTNR_NO
          |""".stripMargin).createOrReplaceTempView("tmp_table1")

      // TODO ?????????  ??????????????????????????????
      spark.sql(
        """
          |select
          |  t1.*,
          |  case t2.CHECK_STATE when '0' then 'Y' else 'N' END as res_2
          |from tmp_table1 t1 left join CL_BIZ_CERT_SCHEDULE t2
          |  on t1.ctnNo = t2.CTN_NO and t1.CertCtnrztnNo = t2.CONTAIN_CERT
          |""".stripMargin).createOrReplaceTempView("tmp_table2")

      //      spark.sql("select * from tmp_table2").show(false)

      // TODO ?????????  ??????????????????
      // todo 1.??????????????????
      spark.sql(
        """
          |select
          |  t1.*,t2.ORG_MSG_NO
          |from tmp_table2 t1 left join DANACK_RECEIPT t2
          |  on t1.vesselDeclarationPreNo = t2.AA_RCP_NO
          |""".stripMargin).createOrReplaceTempView("tmp_table3")

      // todo 2.????????????DANACK_RECEIPT????????? ??????????????????????????????
      spark.sql(
        """
          |select
          |  *
          |from
          |  (
          |  select
          |    ORG_MSG_NO,RCP_DESC,AA_RCP_NO,CREATE_TIME,
          |    rank()over(partition by ORG_MSG_NO order by CREATE_TIME desc) rk
          |  from DANACK_RECEIPT
          |  where RCP_DESC <> '1'
          |  )
          |where rk = 1
          |""".stripMargin).createOrReplaceTempView("DANACK_TMP")

      // todo 3.???????????????????????????????????????????????????????????????????????????
      spark.sql(
        """
          |select
          |  t.*,
          |  if(d.RCP_DESC='2','Y','N') as res_3
          |from tmp_table3 t
          |left join DANACK_TMP d on t.ORG_MSG_NO=d.ORG_MSG_NO
          |""".stripMargin).createOrReplaceTempView("tmp_table4")

      // TODO ?????????  ??????????????????
      // todo 1.??????????????????COPACE?????????????????????????????????????????? ????????????????????????
      spark.sql(
        """
          |select
          |  COPACE.*,
          |  sum(PKG_TTL_NBR)over(partition by VSL_NAME,VOYAGE,VSL_IMO_NO,CTNR_NO,BL_NO rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING) PKG_TTL_NBR_sum,
          |  sum(TTL_WT)over(partition by VSL_NAME,VOYAGE,VSL_IMO_NO,CTNR_NO,BL_NO rows between UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING) TTL_WT_sum
          |from COPACE
          |""".stripMargin).createOrReplaceTempView("dim_copace")

      //        .createOrReplaceTempView("DIM_COPACE")

      // todo ?????? ?????? ????????????
      //      spark.sql(
      //        """
      //          |select
      //          |""".stripMargin)

      val testdf: DataFrame = spark.sql("select * from tmp_table4")

      //      testdf.show(false)

      testdf.rdd

      //      // ???dataframe????????????????????? ???????????????
      //      val arrcolumn: Array[String] = testdf.schema.fieldNames
      //      // ???dataframe??????????????????????????????
      //      val dataMap = new util.HashMap[String, Object]()
      //      for (elem <- arrcolumn) {
      //        //?????????????????????????????????????????? ?????????Map???
      //      }

      //      spark.sql(
      //        """
      //          |select
      //          |  vslName,
      //          |  voyage,
      //          |  billNo,
      //          |  ctnNo,
      //          |  sum(CtnrGrossWt),
      //          |  sum(PkgQtyInCtnr)
      //          |from tmp_table2 group by vslName,voyage,billNo,ctnNo
      //          |""".stripMargin).show(false)


      //      spark.sql(
      //        """
      //          |select
      //          |  tmp_table2.vslName,
      //          |  tmp_table2.voyage,
      //          |  tmp_table2.billNo,
      //          |  tmp_table2.ctnNo,
      //          |  tmp_table2.CtnrGrossWt,
      //          |  tmp_table2.PkgQtyInCtnr
      //          |from tmp_table2 left join COPACE on tmp_table2.CertCtnrztnNo=COPACE.CTNR_PACKING_CERT_NO and tmp_table2.ctnNo=COPACE.CTNR_NO
      //          |""".stripMargin).show(false)
    })


    ssc.start()
    ssc.awaitTermination()
  }
}
