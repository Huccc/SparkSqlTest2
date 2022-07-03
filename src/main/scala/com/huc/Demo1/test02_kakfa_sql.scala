package com.huc.Demo1

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object test02_kakfa_sql {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder().master("local[*]").appName(this.getClass.getSimpleName).getOrCreate()

    session.sql(
      """
        |CREATE TABLE topic_test
        |USING kafka
        |OPTIONS (
        |  kafka.bootstrap.servers '192.168.129.121:9092,192.168.129.122:9092,192.168.129.123:9092',
        |  subscribe 'eds_source_test',
        |  kafka.group.id 'EDS',
        |  format 'json'
        |)
        |""".stripMargin)

    session.sql(
      """
        |select
        |  *
        |from topic_test
        |""".stripMargin).show(false)

    session.close()
  }

}
