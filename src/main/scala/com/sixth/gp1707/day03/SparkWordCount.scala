package com.sixth.gp1707.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ Author ：liuhao
  * @ Date   ：Created in 14:16 2018/7/25
  * @ 
  */
object SparkWordCount {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
    conf.setAppName("SparkWordCount")
//      .setAppName(this.getClass.getName)
      .setMaster("local[*]")
//      .set("fs.defaultFS", "hdfs://liuhao")

    // 创建上下文对象，也成为集群的入口类
    val sc: SparkContext = new SparkContext(conf)

    // 获取数据
//    val lines: RDD[String] = sc.textFile("hdfs://liuhao/1.sh")
    val lines: RDD[String] = sc.textFile("E:\\QF\\spark\\sparkcoursesinfomation\\project\\logmonitor\\log.txt")
//    sc.parallelize()
    // new ParallelCollectionRDD()
//    lines.foreachPartition()

    // 将数据切分并压平
    val words: RDD[String] = lines.flatMap(_.split(" "))

    // 将单词生成元组
    val tuples: RDD[(String, Int)] = words.map((_, 1))

    // 将元组以key来聚合
    val sumed: RDD[(String, Int)] = tuples.reduceByKey(_+_)

    // 将结果进行降序排序
    // f:(T) => K
    // 第二个参数指定是按照升序排序
    // 第三个参数是设置分区数
    val sorted: RDD[(String, Int)] = sumed.sortBy(_._2, false)

    // 打印该RDD的分区数
    println(sorted.partitions.size)

    // 打印或存储
    // 目录会自动生成
//    sorted.saveAsTextFile("hdfs://liuhao:9000/out/spark/01")
    sorted.saveAsTextFile(args(1))
//    println(sorted.collect().toBuffer)

    // 回收资源
    sc.stop()
  }

}
