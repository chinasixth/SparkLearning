package com.sixth.group07.day06

import org.apache.spark.rdd.RDD
import org.apache.spark.{Accumulator, SparkConf, SparkContext}

/**
  * @ Author ：liuhao
  * @ Date   ：Created in 16:36 2018/7/30
  * @ 
  */
object Accumulator {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Accumulator").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val numbers: RDD[Int] = sc.parallelize(Array(6,5,4,3,2,1), 2)

    // 累加器
    val sum: Accumulator[Int] = sc.accumulator(0)
//    如果使用一般的数据去累加，最后的结果是0
//    var sum = 0;

    // 累加到sum
    numbers.foreach(number => sum.+=(number))

    println(sum)
  }
}
