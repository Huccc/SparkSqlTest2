package com.huc.Demo1

import org.apache.spark.sql.SparkSession


object test05_time {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("test05").getOrCreate()

    spark.sql("select unix_timestamp('2020-11-25','yyyy-MM-dd') as unix").show()

    var test5 = spark.sql(s"select to_timestamp(to_date('2022-07-01 15:00:00','yyyy-MM-dd HH:mm:ss'))  as times")

    test5.show()

    spark.close()
  }

}
