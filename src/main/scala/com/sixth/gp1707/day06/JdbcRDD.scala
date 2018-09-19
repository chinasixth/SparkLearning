package com.sixth.gp1707.day06

import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ Author ：liuhao
  * @ Date   ：Created in 16:20 2018/7/28
  * @ 
  */
object JdbcRDD {
  def main(args: Array[String]): Unit = {
    // 模板代码
    val conf: SparkConf = new SparkConf().setAppName("JdbcRDD")
    val sc = new SparkContext(conf)
    val sqlContext: SQLContext = new SQLContext(sc)
    // 通过并行化创建RDD
    val personRDD: RDD[Array[String]] = sc.parallelize(Array("6 yeer 22")).map(_.split(" "))
    // 通过StructType直接指定每个字段的schema
    val schema: StructType = StructType(
      List(
        StructField("id", IntegerType, true),
        StructField("name", StringType, true),
        StructField("age", IntegerType, true)
      )
    )

    // 将RDD映射到rowRDD上
    val rowRDD: RDD[Row] = personRDD.map(p => Row(p(0).toInt, p(1).trim, p(2).toInt))

    // 将schema信息应用到rowRDD上
    val personDataFrame: DataFrame = sqlContext.createDataFrame(rowRDD, schema)

    // 创建Properties
    val prop: Properties = new Properties()
    prop.put("user", "root")
    prop.put("password", "123456")

    // 将数据追加到数据库
    personDataFrame.write.mode("append").jdbc("jdbc:mysql://hadoop05:3306/test", "test.person", prop)

    // 停止SparkContext
    sc.stop()
  }

}
