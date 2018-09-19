package com.sixth.gp1707.day09.copy

import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @ Author ：liuhao
  * @ Date   ：Created in 23:02 2018/8/2
  * @ 
  */
object LoadKafKaWordCount {
  def main(args: Array[String]): Unit = {
    /*
    * 需求：从kafka中读取数据，然后进行自己想要的操作
    * 本质：Spark Streaming处理实时数据
    * 技术点：连接kafka，处理历史记录
    * 实现思路：
    * 既然是Spark Streaming，同样是写模板代码，获取StreamingContext对象
    * 拿到DStream数据进行操作，当最后进行reduce操作时，因为要用到历史记录，所以使用updateStateByKey
    * */
    val conf = new SparkConf()
      .setAppName("LoadKafKaWordCount")
      .setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))

    // 指定kafka集群、消费者组名、topic、线程数，以args参数的方式传入
    val Array(zkQuorum, group, topics, numThreads) = args

    // 需要将topics变成Map类型的，value为线程数，至于为什么要指定线程数，自己想.使用几个线程读取topic
    val topicsMap: Map[String, Int] = topics.split(",").map((_, numThreads.toInt)).toMap

    // 使用Kafka的工具类，连接kafka集群，获取到数据并返回
    val dStream: ReceiverInputDStream[(String, String)] =
      KafkaUtils.createStream(ssc, zkQuorum, group, topicsMap, StorageLevel.MEMORY_AND_DISK)

    // 拿到的数据不只是我们想要的数据，还有索引值，第一个是索引，第二个是我们需要的值，所以需要对数据进行处理
    val lines: DStream[String] = dStream.map(_._2)

    val tupled: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_, 1))
    val summed: DStream[(String, Int)] = tupled.updateStateByKey(func, new HashPartitioner(1), true)
    summed.print()

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
