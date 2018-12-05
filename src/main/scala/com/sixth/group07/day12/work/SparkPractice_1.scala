package com.sixth.group07.day12.work

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
  * @ Author ：liuhao
  * @ Date   ：Created in 8:30 2018/8/8
  * @ 
  */
object SparkPractice_1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setAppName("SparkPractice")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile("E:\\QF\\spark\\day12\\testdata.txt")

    val lineWords: RDD[Array[String]] = lines.map(_.split(" "))
    val mapped: RDD[(Array[String], Int)] = lineWords.map(x => {
      var flag = 0
      if (x.toList.contains("Spark")) {
        flag = 1
      }
      (x, flag)
    })

    val filtered: RDD[(Array[String], Int)] = mapped.filter(_._2 == 1)
    val counted: Long = filtered.count()

    val value: RDD[Array[String]] = filtered.map(_._1)
    val array: Array[Array[String]] = value.collect()
    array.foreach(x => {
      println(x.toBuffer)
    })

    println(counted)

    sc.stop()
  }
}
