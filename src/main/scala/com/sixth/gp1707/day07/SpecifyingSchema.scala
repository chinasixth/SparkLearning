package com.sixth.gp1707.day07

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ Author ：liuhao
  * @ Date   ：Created in 15:12 2018/7/28
  * @ 通过StructType指定Schema信息
  */
object SpecifyingSchema {
  def main(args: Array[String]): Unit = {
    // 创建SparkConf并设置AppName
    val conf: SparkConf = new SparkConf().setAppName("SpecifyingSchema")
    // 创建SQLContext依赖SparkContext
    val sc: SparkContext = new SparkContext(conf)
    // 创建SQLContext
    val sQLContext = new SQLContext(sc)
    // 从指定文件中创建RDD
    val personRDD: RDD[Array[String]] = sc.textFile(args(0)).map(_.split(" "))
    // 通过StructType直接指定每个字段的schema
    val schema: StructType = StructType(
      List(
        StructField("id", IntegerType, true),
        StructField("name", StringType, true),
        StructField("age", IntegerType, true)
      )
    )
    // 将RDD映射成rowRDD
    // Row可以封装任意类型的值
    val rowRDD: RDD[Row] = personRDD.map(p => Row(p(0).toInt, p(1).trim, p(2).toInt))
    // 将schema信息应用到rowRDD上
    val personDataFrame: DataFrame = sQLContext.createDataFrame(rowRDD, schema)
    // 注册成表
    personDataFrame.registerTempTable("t_person")
    // 指定SQL
    val df: DataFrame = sQLContext.sql("select * from t_person order by age desc limit 2")
    // 将结果以JSON的方式存储到指定的位置
    // 如果没有person目录，会自动创建的
    //    df.write.mode("append").json("d://person")
    df.write.json(args(1))
    // 停止SparkContext
    sc.stop()
  }

}
