package com.sixth.gp1707.day08.sparkstreaming

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ Author ：liuhao
  * @ Date   ：Created in 16:45 2018/8/1
  * @ 用TransForm原语可以用算子操作DStream包含的RDD
  */
object TransFormDemo {
  // 在方法内的任意一个位置ctrl + w 连续三次，即可选中整个方法
  // ctrl + alt + m 可以对代码进行重构
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TransFormDemo").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))

    val dStream = ssc.socketTextStream("hadoop05", 6666)
    // 在transForm中，传入的是RDD，所以可以在这个原语中，直接使用RDD的算子，返回的类型仍然是DStream
    val summed: DStream[(String, Int)] = dStream.transform(rdd => rdd.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _))

    summed.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
