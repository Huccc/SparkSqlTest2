package com.huc.SparkSql

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object test02_RDD_DF {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("sparksql").setMaster("local[*]")

    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // 创建一个RDD
    val sc: SparkContext = session.sparkContext


    val rdd: RDD[String] = sc.textFile("input/user.txt")

    // 对三者进行相互转换，需要使用sparkSession隐式转换
    import session.implicits._

    // 将rdd转化为DataFrame
    // 默认情况下df只有一列value
    val dataframe: DataFrame = rdd.toDF()

    dataframe.show()

    // 对rdd进行转化为二元组
    val tupleRdd: RDD[(String, String)] = rdd.map(s => {
      val data: Array[String] = s.split(",")
      (data(0), data(1))
    })

    val dataFrame1: DataFrame = tupleRdd.toDF("name", "age")

    dataFrame1.show()

    dataFrame1.createOrReplaceTempView("test")

    session.sql(
      """
        |select
        | name
        |from test
        |""".stripMargin).show()

    // 将df转换为rdd
    val rdd1: RDD[Row] = dataFrame1.rdd

    for (elem <- rdd1.collect()) {
      print(elem.getString(0) + "--")
      println(elem.getString(1))
    }

    // 使用样例类相互转换
    // 如果rdd是样例类的数据类型，转换为df的时候会将样例类的属性读取为列
    val userRdd: RDD[User] = sc.makeRDD(List(User("zhangsan", 10), User("lisi", 20)))
    val frame: DataFrame = userRdd.toDF()
    frame.show()

    val rdd2: RDD[Row] = frame.rdd
    // 如果使用样例类的df转换为rdd，会丢失数据类型
    rdd2.collect().foreach(println)

    // 转换为DataSet


    session.close()
  }

  case class User(name: String, age: Int) {

  }

}
