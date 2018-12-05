package com.sixth.group07.day09

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf}

/**
  * @ Author ：liuhao
  * @ Date   ：Created in 9:49 2018/8/2
  * @ 获取kafka的数据，并作批次累加功能的单词计数
  * 注意：
  * 1.如何设置获取kafka的配置信息
  * 2.如何调用updateStateByKey原语
  */
object LoadKafkaDataAndWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("LoadKafkaDataAndWordCount")
      .setMaster("local[2]")
    // 隐式生成sc
    val ssc = new StreamingContext(conf, Seconds(5))

    // 实现批次累加功能需要checkpoint
    ssc.checkpoint("c://cp-20180802-1")

    // 设置请求kafka的必要配置信息
    // zk集群，组名，获取哪些topic，几个线程
    // 通过参数传入
    val Array(zkQuorum, group, topics, numThreads) = args

    // 把每个topic放到一个map中, 传参的时候，topic之间用","分隔
    // 注意args传入的参数都是string类型的，所以要toInt
    val topicMap: Map[String, Int] =
    topics.split(",").map((_, numThreads.toInt)).toMap

    // 调用kafka工具类获取kafka集群的数据,敲重点、画黑板
    // 这里就将SparkStreaming和kafka结合连接，根据指定的topicMap获取到数据并返回
    // 返回的数据中是以offset作为key，data作为value的
    val data: ReceiverInputDStream[(String, String)] =
      KafkaUtils.createStream(ssc, zkQuorum, group, topicMap, StorageLevel.MEMORY_AND_DISK)

    // 因为key对应的是offset值，不需要，只保留数据即可
    val lines: DStream[String] = data.map(_._2)

    // 开始计算
    val tupled: DStream[(String, Int)] =
      lines.flatMap(_.split(" ")).map((_, 1))

    //
    val summed: DStream[(String, Int)] =
      tupled.updateStateByKey(func,
        new HashPartitioner(ssc.sparkContext.defaultParallelism),
        true)

    summed.print()
    ssc.start()
    ssc.awaitTermination()
  }

  val func = (it: Iterator[(String, Seq[Int], Option[Int])]) => {
    //    it.map(x => {
    //
    //    })
    // 使用模式匹配的方式,注意使用{}
    it.map {
      // 注意x,y,z分别代表什么
      case (x, y, z) => {
        (x, y.sum + z.getOrElse(0))
      }
    }
  }
}
