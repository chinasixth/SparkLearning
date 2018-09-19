package com.sixth.gp1707.day08.sparkstreaming

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf}

/**
  * @ Author ：liuhao
  * @ Date   ：Created in 16:02 2018/8/1
  * @ 实现按批次进行累加的功能
  * 需要用到UpdateStateByKey
  */
object SparkStreamingACCWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkStreamingACCWordCount").setMaster("local[2]")
    // 隐式调用了
    val ssc = new StreamingContext(conf, Milliseconds(5000))
    // 检查点最好设置到hdfs上
    // 没运行一次，就会记录一次内容，数据会积累的越来越多，最好清除
    ssc.checkpoint("c://wc-20180801-1")

    // 获取数据
    val dStream = ssc.socketTextStream("192.168.216.115", 6666)

    // 计算任务
    // 这里不能使用reduceByKey，因为reduceByKey只将当前批次的结果进行聚合
    val tupled: DStream[(String, Int)] = dStream.flatMap(_.split(" ")).map((_, 1))
    // 调用updateStateByKey，当前批次及之前的结果进行聚合
    // 因为updateStateByKey不能对以前批次的结果进行保存，所以要设置保存点（checkPoint）
    // 默认分区数，还有一个是默认最小的分区数
    // 是否记住分区器
    val summed: DStream[(String, Int)] = tupled.updateStateByKey(func, new HashPartitioner(ssc.sparkContext.defaultParallelism), true)

    summed.print()
    ssc.start()
    ssc.awaitTermination()
  }

  // 传入的函数中参数的含义：参数中只有一个迭代器，其中有三个类型，含义如下：
  // 第一个代表元组中的每一个单词
  // 第二个Seq是代表当前批次相同单词出现的次数，如：Seq(1,1,1,1)
  // 第三个Option代表上一批次相同单词累加的结果，有可能有值，也有可能没有值，所以要调用Option来封装，此时取值最好使用getOrElse方法
  val func = (it: Iterator[(String, Seq[Int], Option[Int])]) => {
    it.map(x => {
      // 这里不使用get是因为，如果没有值，返回的是None
      (x._1, x._2.sum + x._3.getOrElse(0))
    })
  }
}
