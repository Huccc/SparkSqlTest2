package com.huc.SparkSql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

object test01_df_json {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparksql")
    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val df2: DataFrame = session.read.json("input/test2.json")

    df2.createOrReplaceTempView("test2")

    val props: Properties = new Properties()
    props.setProperty("user", "xqwu")
    props.setProperty("password", "easipass")

    val df1: DataFrame = session.read.jdbc("jdbc:oracle:thin:@192.168.129.149:1521/test12c", "DM.TRACK_BIZ_STATUS_BILL", props)

    df1.createOrReplaceTempView("TRACK_BIZ_STATUS_BILL")


    session.sql(
      """
        |select
        |  VSL_IMO_NO,VSL_NAME,BL_NO,current_timestamp
        |from
        |  TRACK_BIZ_STATUS_BILL as t
        |where
        |  t.VSL_IMO_NO <> 'N/A'
        |""".stripMargin).createOrReplaceTempView("t1")

    session.sql(
      """
        |select
        |  *
        |from
        |  test2
        |""".stripMargin)

    session.sql(
      """
        |select a1.RecId1,a1.VesselNo,a2.RecId2,a2.MsgType,a3.RecId3,a3.AppendixType,a5.RecId5,a6.RecId6,a7.RecId7,a8.RecId8,a8.RecTtlQty from test2
        |lateral view json_tuple(to_json(VesselVoyageInformation),'RecId1','VesselNo') a1 as RecId1,VesselNo
        |lateral view json_tuple(to_json(HeadRecord),'RecId2','MsgType') a2 as RecId2,MsgType
        |lateral view explode(split(regexp_replace(regexp_extract(to_json(AppeneixInformation),'^\\[(.+)\\]$',1),'\\}\\,\\{', '\\}\\|\\|\\{'),'\\|\\|')) AppeneixInformation as a3_tmp
        |lateral view json_tuple(a3_tmp,'RecId','AppendixType') a3 as RecId3,AppendixType
        |lateral view explode(split(regexp_replace(regexp_extract(to_json(BillOfLadingInformation),'^\\[(.+)\\]$',1),'\\}\\,\\{', '\\}\\|\\|\\{'),'\\|\\|')) AppeneixInformation as a4_tmp
        |lateral view json_tuple(a4_tmp,'RecId3','BlNo','DangerousCargoInformation') a4 as RecId4,BlNo,DangerousCargoInformation
        |lateral view explode(split(regexp_replace(regexp_extract(DangerousCargoInformation,'^\\[(.+)\\]$',1),'\\}\\,\\{', '\\}\\|\\|\\{'),'\\|\\|')) AppeneixInformation as a5_tmp
        |lateral view json_tuple(a5_tmp,'RecId','DgrgdAuditRcpNo','PackagesInformation','UnitInformation') a5 as RecId5,DgrgdAuditRcpNo,PackagesInformation,UnitInformation
        |lateral view explode(split(regexp_replace(regexp_extract(PackagesInformation,'^\\[(.+)\\]$',1),'\\}\\,\\{', '\\}\\|\\|\\{'),'\\|\\|')) AppeneixInformation as a6_tmp
        |lateral view json_tuple(a6_tmp,'RecId','PackagesNo') a6 as RecId6,PackagesNo
        |lateral view explode(split(regexp_replace(regexp_extract(UnitInformation,'^\\[(.+)\\]$',1),'\\}\\,\\{', '\\}\\|\\|\\{'),'\\|\\|')) AppeneixInformation as a7_tmp
        |lateral view json_tuple(a7_tmp,'RecId','TypeOfUnit') a7 as RecId7,TypeOfUnit
        |lateral view json_tuple(to_json(TailRecord),'RecId','RecTtlQty') a8 as RecId8,RecTtlQty
        |""".stripMargin).createOrReplaceTempView("t2")

    session.sql(
      """
        |select
        |  t1.*,t2.RecId1
        |from t1 join t2
        |on t1.VSL_IMO_NO=t2.RecId1
        |""".stripMargin).show(false)

    session.close()
  }

}
