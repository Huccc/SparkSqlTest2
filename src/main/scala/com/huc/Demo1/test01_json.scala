package com.huc.Demo1

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

object test01_json {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparksql")
    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val sc: SparkContext = session.sparkContext

    val rdd: RDD[String] = sc.textFile("input/test.json")

    import session.implicits._
    val df: DataFrame = rdd.toDF

    val dataFrame: DataFrame = session.read.json("input/test.json")

    dataFrame.createOrReplaceTempView("test")

    // 编写sql查询数据
    session.sql(
      """
        |select
        |  *
        |from
        |  test
        |""".stripMargin).show(false)

    session.sql( """
                   |select username,ai.id,ai.age,p.uname,p.code from test
                   |lateral view json_tuple(to_json(actionInfo),'id','age','partList') ai as id,age,partlist
                   |lateral view explode(split(regexp_replace(regexp_extract(partlist,'^\\[(.+)\\]$',1),'\\}\\,\\{', '\\}\\|\\|\\{'),'\\|\\|')) partlist as p
                   |lateral view json_tuple(p,'code','uname') p as code,uname
""".stripMargin).show(false)

    session.close()
  }

}
