package com.sixth.group07.day08.sparkstreaming.cpoy

import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

/**
  * @ Author ：liuhao
  * @ Date   ：Created in 16:15 2018/9/20
  * @ 
  */
object SparkStreamingWC {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("SparkStreamingWC")
      .setMaster("local[*]")
    val ssc = new StreamingContext(conf, Milliseconds(5000))

    ssc.checkpoint("c:/2018-09-20")
    val ds: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.216.115", 6666, StorageLevel.MEMORY_AND_DISK)
    val dss: DStream[String] = ds.flatMap(_.split(" "))
    val mapDS: DStream[(String, Int)] = dss.transform(rdd => rdd.map((_, 1)))
    val update: DStream[(String, Int)] = mapDS.updateStateByKey(func, new HashPartitioner(ssc.sparkContext.defaultParallelism), true)
    update.print()

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
