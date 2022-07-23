package com.huc.Ddmo5

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
    val offset= """ {"topicA":{"0": 1, "1": 1}, "topicB": {"0": 1, "1": 1}} """

    spark.sql(
      s"""
         |CREATE TABLE kafka_data_exchange_kafka_topic
         |USING kafka
         |OPTIONS (
         |  kafka.bootstrap.servers '192.168.129.121:9092,192.168.129.122:9092,192.168.129.123:9092',
         |  subscribe 'test_eds1',
         |  --format 'json',
         |  --startingOffsets \"\"\"{"topic1":{"0":23,"1":-2},"topic2":{"0":-2}}\"\"\",
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

    // TODO 拆解BillOfLadingInformation  获取提单号
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

    // TODO 拆解DangerousCargoInformation  获取获取危险品预校验号MsaDeclAudtNo
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

    // TODO 拆解UnitInformation  取箱号，集装箱声明单编号，组件毛重，箱内货物件数
    // todo 当UnitType为2.1时，ctnNo为箱号
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
      get_json_object(col("HeadRecord"), "$.FileFun").alias("FileFun"), // 文件功能
      get_json_object(col("HeadRecord"), "$.SenderCode").alias("SenderCode"), // 发送方代码
      get_json_object(col("HeadRecord"), "$.RecipientCode").alias("RecipientCode"), // 接收方代码
      get_json_object(col("HeadRecord"), "$.FileCreateTime").alias("FileCreateTime"), // 报文生成时间
      get_json_object(col("HeadRecord"), "$.MsgType").alias("MsgType"), // 报文类型
      get_json_object(col("HeadRecord"), "$.FileDesp").alias("FileDesp"), // 报文说明
      get_json_object(col("VesselAndVoyageInformation"), "$.VslName").alias("VslName"), // 船名
      get_json_object(col("VesselAndVoyageInformation"), "$.Voyage").alias("Voyage"), // 航次
      col("MsaDeclAudtNo"), // 危险货物船申报预校验号
      col("UnitIdNo"), // 箱号
      col("UnitType"), // 当UnitType为2.1时，ctnNo为箱号
      col("CertCtnrztnNo"), // 集装箱申明单编号
      col("CtnrGrossWt"), // 组件毛重
      col("PkgQtyInCtnr"), // 箱内货物件数
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
