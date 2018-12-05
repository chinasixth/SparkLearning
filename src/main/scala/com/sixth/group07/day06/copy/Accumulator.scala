package com.sixth.group07.day06.copy

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ Author ：liuhao
  * @ Date   ：Created in 14:45 2018/9/20
  * @ 
  */
object Accumulator {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Accumulator")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(List(1,2,3,4,5,6,7,8,9,10),2)

    val accumulator = sc.accumulator(0)
//    rdd.foreach(accumulator.+=(_))
    rdd.foreachPartition(x => {
      accumulator.+=(x.toList.sum)
    })
    println(accumulator)

    sc.stop()
  }

}
