package com.sixth.gp1707.day09

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @ Author ：liuhao
  * @ Date   ：Created in 11:23 2018/8/2
  * @ 
  */
object WindowOperationsWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("WindowOperationsWordCount")
      .setMaster("local[2]")
//    val sc = new SparkContext(conf)
//    val ssc = new StreamingContext(sc, Seconds(5))

    val ssc = new StreamingContext(conf, Seconds(5))

    //
    ssc.checkpoint("c://cp-20180802-2")

    val dStream = ssc.socketTextStream("hadoop05", 6666)
    val tupled: DStream[(String, Int)] = dStream.flatMap(_.split(" ")).map((_, 1))

    // 一般在聚合的时候调用window原语
    // 最后有一个分区的参数，可以不写
    val res: DStream[(String, Int)] = tupled.reduceByKeyAndWindow((x:Int, y:Int) => x + y, Seconds(10), Seconds(10))
    res.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
