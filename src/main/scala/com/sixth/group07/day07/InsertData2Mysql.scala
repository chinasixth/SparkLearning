package com.sixth.group07.day07

import java.util.Properties

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ Author ：liuhao
  * @ Date   ：Created in 14:10 2018/7/31
  * @ 用sparkSQL的方式将数据写入数据库
  */
object InsertData2Mysql {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("InsertData2Mysql").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // 获取数据并切分
    val linesRDD = sc.textFile("hdfs://liuhao/").map(_.split(","))

    // 生成schema信息
    val schema = StructType {
      Array(
        StructField("name", StringType, true),
        StructField("age", IntegerType, true),
        StructField("fv", IntegerType, true)
      )
    }

    // 映射
    val personRDD = linesRDD.map(p => Row(p(1), p(2).toInt, p(3).toInt))

    // 转换为DataFrame
    val personDF = sqlContext.createDataFrame(personRDD, schema)

    // 配置用于请求mysql的一些配置信息
    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "123456")
    prop.put("driver", "com.mysql.jdbc.Driver")
    val url = "jdbc:mysql://192.168.216.115:3306/bigdata"
    val table = "person"

    // 把数据写入到数据库
    personDF.write.mode("append").jdbc(url, table, prop)

    sc.stop()
  }
}
