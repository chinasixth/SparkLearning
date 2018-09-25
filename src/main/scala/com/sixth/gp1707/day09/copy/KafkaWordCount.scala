package com.sixth.gp1707.day09.copy

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * @ Author ：liuhao
  * @ Date   ：Created in 17:11 2018/9/20
  * @ 
  */
object KafkaWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("KafkaWordCount")
      .setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))

    ssc.checkpoint("c:/20180820-001")

    val Array(zkQuroum, group, topics, numThreads) = args
    val topicMap: Map[String, Int] = topics.split(",").map((_, numThreads.toInt)).toMap

    val receiveDS: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc, zkQuroum, group, topicMap, StorageLevel.MEMORY_AND_DISK)
    val wordDS: DStream[(String, Int)] = receiveDS.flatMap(_._2.split(" ")).map((_, 1))

    val updateDS: DStream[(String, Int)] = wordDS.updateStateByKey(func, new HashPartitioner(ssc.sparkContext.defaultParallelism), true)
    updateDS.print()

    ssc.start()
    ssc.awaitTermination()
  }

  val func = (it: Iterator[(String, Seq[Int], Option[Int])]) => {
    it.map {
      case (x, y, z) => {
        (x, y.sum + z.getOrElse(0))
      }
    }
  }

}
