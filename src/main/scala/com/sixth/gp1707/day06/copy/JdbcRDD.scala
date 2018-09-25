package com.sixth.gp1707.day06.copy

import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ Author ：liuhao
  * @ Date   ：Created in 15:15 2018/9/20
  * @ 
  */
object JdbcRDD {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("JdbcRDD")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val rdd = sc.parallelize(Array("6 ye 22")).map(_.split(" "))
    val schema = StructType(
      List(
        StructField("id", IntegerType, true),
        StructField("name", StringType, true),
        StructField("age", IntegerType, true)
      )
    )

    val map: RDD[Row] = rdd.map(x => Row(x(0).toInt, x(1), x(2).toInt))
    val df: DataFrame = sqlContext.createDataFrame(map, schema)

    val prop = new Properties()
    prop.put("username", "root")
    prop.put("password", "123456")

    val url = "jdbc:mysql://hadoop05:3306/test"
    df.write.jdbc(url, "test.person", prop)


    sc.stop()
  }

}
