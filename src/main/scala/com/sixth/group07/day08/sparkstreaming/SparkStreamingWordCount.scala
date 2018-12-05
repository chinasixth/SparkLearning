package com.sixth.group07.day08.sparkstreaming

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ Author ：liuhao
  * @ Date   ：Created in 15:05 2018/8/1
  * @ 
  */
object SparkStreamingWordCount {
  def main(args: Array[String]): Unit = {
    // netcat中至少需要两个线程，因为一个线程用来接收数据，一个线程处理数据
    val conf = new SparkConf().setAppName("SparkStreamingWC").setMaster("local[2]")
    val sc = new SparkContext(conf)
    // 创建SparkStreaming的上下文对象
    // 第二个参数是批次间隔
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(5))

    // 获取NatCat服务端的数据
    val dStream: ReceiverInputDStream[String] =
      ssc.socketTextStream("192.168.216.115", 6666)
    // 处理数据
    val summed: DStream[(String, Int)] =
      dStream.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_)

    // 实时结果打印，每过5秒打印一次
    summed.print()

    // 提交任务到集群
    ssc.start()

    // 线程等待,等待处理下一批次任务
    ssc.awaitTermination()
  }
}
