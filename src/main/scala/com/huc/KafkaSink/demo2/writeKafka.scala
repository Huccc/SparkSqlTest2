package com.huc.KafkaSink.demo2

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.google.gson.Gson
import com.huc.utils.{KafkaSink, KafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{ProducerConfig, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.{SPARK_BRANCH, SparkConf}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}

import java.util.Properties
import java.util.concurrent.Future
import scala.collection.mutable.ArrayBuffer
import com.alibaba.fastjson
import com.alibaba.fastjson.serializer.SerializerFeature
import org.apache.commons.lang.StringEscapeUtils
import org.apache.spark.rdd.RDD

/**
 * Created with IntelliJ IDEA.
 *
 * @Project : SparkSqlTest2
 * @Package : com.huc.KafkaSink.demo2
 * @createTime : 2022/7/23 16:20
 * @author : huc
 * @Email : 1310259975@qq.com
 * @Description : 
 */
object writeKafka {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("test01")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")


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

    import spark.implicits._

    val stream: DStream[Row] = value.transform(rdd => {
      val dfKafka: DataFrame = rdd.toDF("msgId", "bizId", "msgType", "bizUniqueId", "destination", "FunctionCode", "SenderCode", "ReceiverCode", "FileCreateTime", "MessageType", "Description", "vslName", "voyage", "VslImoNo", "billNo", "vesselDeclarationPreNo", "ctnNo", "UnitType", "CertCtnrztnNo", "CtnrGrossWt", "PkgQtyInCtnr", "CtnrSizeType")

      dfKafka.createOrReplaceTempView("dfKafka")
      // TODO ?????????  ???????????????????????????
      val df: DataFrame = spark.sql(
        """
          |select
          |  dfKafka.*,if(COPACE.FILE_NAME <> '','Y','N') as res_1
          |from dfKafka left join COPACE on dfKafka.CertCtnrztnNo=COPACE.CTNR_PACKING_CERT_NO and dfKafka.ctnNo=COPACE.CTNR_NO
          |""".stripMargin)
      df.rdd
    })


    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "192.168.129.121:9092,192.168.129.122:9092,192.168.129.123:9092") //???????????????kafka??????
    properties.setProperty("key.serializer", classOf[StringSerializer].getName)
    properties.setProperty("value.serializer", classOf[StringSerializer].getName)

    def getKafkaProducerParams(): Map[String, Object] = {
      Map[String, Object](
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> properties.getProperty("bootstrap.servers"),
        //        ProducerConfig.ACKS_CONFIG -> properties.getProperty("kafka1.acks"),
        //        ProducerConfig.RETRIES_CONFIG -> properties.getProperty("kafka1.retries"),
        //        ProducerConfig.BATCH_SIZE_CONFIG -> properties.getProperty("kafka1.batch.size"),
        //        ProducerConfig.LINGER_MS_CONFIG -> properties.getProperty("kafka1.linger.ms"),
        //        ProducerConfig.BUFFER_MEMORY_CONFIG -> properties.getProperty("kafka1.buffer.memory"),
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer],
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer]
      )
    }

    val gson = new Gson()

    // ?????????KafkaSink,?????????
    val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
      val kafkaProducerConfig: Map[String, Object] = getKafkaProducerParams()
      //      if (logger.isInfoEnabled){
      //        logger.info("kafka producer init done!")
      //      }
      ssc.sparkContext.broadcast(KafkaSink[String, String](kafkaProducerConfig))
    }

    stream.foreachRDD((rdd: RDD[Row]) => {
      rdd.foreachPartition((partitionOfRecords: Iterator[Row]) => {
        val metadata: Stream[Future[RecordMetadata]] = partitionOfRecords.map(record => {
          val json: String = record.json
          kafkaProducer.value.send("my-output-topic", json)
        }).toStream
        metadata.foreach((data: Future[RecordMetadata]) => {
          data.get()
        })
      })
    })

    //    stream.foreachRDD(rdd => {
    //      val value1: RDD[Future[RecordMetadata]] = rdd.map(record => {
    //        //        val str: String = JSON.toJSONString(record,SerializerFeature.DisableCircularReferenceDetect)
    //
    //        val str: String = gson.toJson(return)
    //        val jsonarr: JSONObject = JSON.parseObject(str).getJSONArray("values").getJSONObject(0)
    //        val string: String = JSON.toJSONString(jsonarr, SerializerFeature.WriteNullListAsEmpty,
    //          SerializerFeature.WriteNullStringAsEmpty,
    //          SerializerFeature.WriteDateUseDateFormat,
    //          SerializerFeature.WriteNullNumberAsZero,
    //          SerializerFeature.WriteNullBooleanAsFalse,
    //          SerializerFeature.DisableCircularReferenceDetect)
    //
    //        kafkaProducer.value.send("my-output-topic", string)
    //      })
    //      value1.foreach((data: Future[RecordMetadata]) => {
    //        data.get()
    //      })
    //    })

    ssc.start()
    ssc.awaitTermination()
  }
}
