package com.sixth.gp1707.day07

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ Author ：liuhao
  * @ Date   ：Created in 14:15 2018/7/28
  * @ 通过反射推断出schema信息
  */
object InferringSchema {
  def main(args: Array[String]): Unit = {
    // 创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("InferringSchema")
    // SQLContext要依赖SparkContext
    val sc: SparkContext = new SparkContext(conf)
    // 创建SQLContext
    val sqlContext: SQLContext = new SQLContext(sc)
    // 从指定地址获取数据并切分数据来创建RDD
    val lineRDD: RDD[Array[String]] = sc.textFile(args(0)).map(_.split(" "))

    // 创建case class
    // 将RDD和case class关联
    val personRDD: RDD[Person] = lineRDD.map(x => Person(x(0).toInt, x(1), x(2).toInt))
    // 导入隐式转换，如果不导入无法将RDD转换成DataFrame
    // 将RDD转换成DataFrame
    import sqlContext.implicits._
    val personDF: DataFrame = personRDD.toDF()
    // 注册成表
    personDF.registerTempTable("t_person")
    // 传入SQL，一般都用sql的方式来使用spark sql
    val df: DataFrame = sqlContext.sql("select * from t_person order by age desc limit 2")
    // 将结果以JSON的方式存储到指定位置
    // 数据写入模式，是overwrite还是append、error、ignore
    // parquet列式存储，是一种存储格式
    df.write.mode("append").json("")
    df.write.json(args(1))
    // 停止SparkContext
    sc.stop()
  }
}

// case class 一定要放在外面
case class Person(id: Int, name: String, age: Int)
