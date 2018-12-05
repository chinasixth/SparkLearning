package com.sixth.group07.day03

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ Author ：liuhao
  * @ Date   ：Created in 21:04 2018/9/17
  * @ 
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("wc")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)
    val lines = sc.parallelize(Seq("hello world hello tuoer"))
    val words = lines.flatMap(_.split(" "))
    val tuple = words.map((_, 1))
    val reduced = tuple.reduceByKey(_+_)
    print(reduced.collect().toBuffer)
    sc.stop()
  }
}
