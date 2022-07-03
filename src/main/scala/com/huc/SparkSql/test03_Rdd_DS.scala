package com.huc.SparkSql

import com.huc.SparkSql.test02_RDD_DF.User
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Dataset, SparkSession}

object test03_Rdd_DS {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparksql")
    val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val sc: SparkContext = session.sparkContext

    val userRdd: RDD[User] = sc.makeRDD(List(User("zhangsan", 10), User("lisi", 20)))

    // 需要导入隐式转换
    import session.implicits._
    val ds: Dataset[User] = userRdd.toDS()
    ds.show()

    // 将样例类转换为rdd
    val rdd: RDD[User] = ds.rdd

    for (elem <- rdd.collect()) {
      println(elem.name)
      println(elem.age)
    }

    // 关闭sparksession
    session.close()
  }

}
