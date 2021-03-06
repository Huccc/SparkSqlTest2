package com.huc.Demo5

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Created with IntelliJ IDEA.
 *
 * @Project : SparkSqlTest2
 * @Package : com.huc.Ddmo5
 * @createTime : 2022/7/21 11:12
 * @author : huc
 * @Email : 1310259975@qq.com
 * @Description : 
 */
object sqltest01 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName(this.getClass.getSimpleName).getOrCreate()

    //    session.conf.set("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
//    val offset= """ {"topicA":{"0": 1, "1": 1}, "topicB": {"0": 1, "1": 1}} """

    spark.sql(
      s"""
         |CREATE TABLE kafka_data_exchange_kafka_topic
         |USING kafka
         |OPTIONS (
         |  kafka.bootstrap.servers '192.168.129.121:9092,192.168.129.122:9092,192.168.129.123:9092',
         |  subscribe 'test_eds1',
         |  --format 'json',
         |  --startingOffsets 'earliest',
         |  endingOffsets 'latest'
         |)
         |""".stripMargin)

    val df1: DataFrame = spark.sql(
      """
        |select
        |  topic,partition,offset,timestamp,cast(value as String) as value
        |from kafka_data_exchange_kafka_topic
        |""".stripMargin)
        df1.show(false)
    import org.apache.spark.sql.functions._

    val df2: DataFrame = df1.select(
      col("topic"),
      get_json_object(col("value"), "$.msgId").alias("msgId"),
      get_json_object(col("value"), "$.bizId").alias("bizId"),
      get_json_object(col("value"), "$.msgType").alias("msgType"),
      get_json_object(col("value"), "$.bizUniqueId").alias("bizUniqueId"),
      get_json_object(col("value"), "$.destination").alias("destination"),
      get_json_object(col("value"), "$.parseData").alias("parseData")
    ).filter("bizId <> 'EDSCLR3'")

    //    df2.show(false)
    df2.createOrReplaceTempView("t1")

    val df3: DataFrame = spark.sql(
      """
        |select
        |  tmp.msgId as msgId,
        |  parseData
        |from(
        |select msgId,bizId,msgType,bizUniqueId,destination,explode(from_json(parseData,'array<string>')) as parseData from t1
        |) tmp
        |""".stripMargin)

    //    df3.select(
    //      col("msgId"),
    //      get_json_object(col("parseData"),"$.HeadRecord").alias("HeadRecord")
    //    ).show(false)


    spark.sql(
      """
        |select
        |  msgId,bizId,msgType,bizUniqueId,destination,
        |  json_tuple(t,'HeadRecord','VesselAndVoyageInformation','BillOfLadingInformation') as (HeadRecord,VesselAndVoyageInformation,BillOfLadingInformation)
        |from(
        |select msgId,bizId,msgType,bizUniqueId,destination,explode(from_json(parseData,'array<string>')) as t from t1
        |) tmp
        |""".stripMargin).createOrReplaceTempView("t2")

    // TODO ??????BillOfLadingInformation  ???????????????
    spark.sql(
      """
        |select
        |  msgId,bizId,msgType,bizUniqueId,destination,HeadRecord,VesselAndVoyageInformation,
        |  json_tuple(t,'VslVoyageBlNo','DangerousCargoInformation') as (VslVoyageBlNo,DangerousCargoInformation)
        |from
        |(select
        |  msgId,bizId,msgType,bizUniqueId,destination,HeadRecord,VesselAndVoyageInformation,
        |  explode(from_json(BillOfLadingInformation,'array<string>')) as t
        |from t2)
        |""".stripMargin).createOrReplaceTempView("t3")

    // TODO ??????DangerousCargoInformation  ?????????????????????????????????MsaDeclAudtNo
    spark.sql(
      """
        |select
        |  msgId,bizId,msgType,bizUniqueId,destination,HeadRecord,VesselAndVoyageInformation,VslVoyageBlNo,
        |  json_tuple(t,'MsaDeclAudtNo','UnitInformation') as (MsaDeclAudtNo,UnitInformation)
        |from
        |(select
        |  msgId,bizId,msgType,bizUniqueId,destination,HeadRecord,VesselAndVoyageInformation,VslVoyageBlNo,
        |  explode(from_json(DangerousCargoInformation,'array<string>')) as t
        |from t3)
        |""".stripMargin).createOrReplaceTempView("t4")

    // TODO ??????UnitInformation  ????????????????????????????????????????????????????????????????????????
    // todo ???UnitType???2.1??????ctnNo?????????
    val df4: DataFrame = spark.sql(
      """
        |select
        |  msgId,bizId,msgType,bizUniqueId,destination,HeadRecord,VesselAndVoyageInformation,VslVoyageBlNo,MsaDeclAudtNo,
        |  json_tuple(t,'UnitIdNo','UnitType','CertCtnrztnNo','CtnrGrossWt','PkgQtyInCtnr') as (UnitIdNo,UnitType,CertCtnrztnNo,CtnrGrossWt,PkgQtyInCtnr)
        |from
        |(select
        |  msgId,bizId,msgType,bizUniqueId,destination,HeadRecord,VesselAndVoyageInformation,VslVoyageBlNo,MsaDeclAudtNo,
        |  explode(from_json(UnitInformation,'array<string>')) as t
        |from t4)
        |""".stripMargin)

    df4.select(
      col("msgId"),
      col("bizId"),
      col("msgType"),
      col("bizUniqueId"),
      col("destination"),
      get_json_object(col("HeadRecord"), "$.FileFun").alias("FileFun"), // ????????????
      get_json_object(col("HeadRecord"), "$.SenderCode").alias("SenderCode"), // ???????????????
      get_json_object(col("HeadRecord"), "$.RecipientCode").alias("RecipientCode"), // ???????????????
      get_json_object(col("HeadRecord"), "$.FileCreateTime").alias("FileCreateTime"), // ??????????????????
      get_json_object(col("HeadRecord"), "$.MsgType").alias("MsgType"), // ????????????
      get_json_object(col("HeadRecord"), "$.FileDesp").alias("FileDesp"), // ????????????
      get_json_object(col("VesselAndVoyageInformation"), "$.VslName").alias("VslName"), // ??????
      get_json_object(col("VesselAndVoyageInformation"), "$.Voyage").alias("Voyage"), // ??????
      col("MsaDeclAudtNo"), // ?????????????????????????????????
      col("UnitIdNo"), // ??????
      col("UnitType"), // ???UnitType???2.1??????ctnNo?????????
      col("CertCtnrztnNo"), // ????????????????????????
      col("CtnrGrossWt"), // ????????????
      col("PkgQtyInCtnr"), // ??????????????????
    )
      //      .show(false)
      .createOrReplaceTempView("t5")

    spark.sql(
      """
        |select * from t5
        |""".stripMargin).show(false)

    //    spark.sql(
    //      """
    //        |select
    //        |  t1.msgId,
    //        |  json_tuple(t,'HeadRecord','VesselAndVoyageInformation') as (HeadRecord,VesselAndVoyageInformation)
    //        |from(
    //        |select msgId,bizId,msgType,bizUniqueId,destination,explode(from_json(parseData,'array<string>')) as t from t1
    //        |)
    //        |""".stripMargin).show(false)

    //    spark.sql(
    //      """
    //        |select
    //        |  t.msgId,t.bizId,t.msgType,t.bizUniqueId,t.destination,
    //        |  json_tuple(t,'HeadRecord','VesselAndVoyageInformation') as (HeadRecord,VesselAndVoyageInformation)
    //        |from(
    //        |select msgId,bizId,msgType,bizUniqueId,destination,explode(from_json(parseData,'array<string>')) as t from t1
    //        |)
    //        |""".stripMargin).show(false)

    //    df2.select(
    //      col("topic"),
    //      col("msgId"),
    //      col("bizId"),
    //      col("msgType"),
    //      col("bizUniqueId"),
    //      col("destination"),
    //      explode(from_json(col("parseData",)))
    //    )
    ////      .filter("")
    //      .show(false)

    spark.close()
  }
}
