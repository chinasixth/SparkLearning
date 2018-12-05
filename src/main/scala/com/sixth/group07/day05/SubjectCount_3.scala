package com.sixth.group07.day05

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

/**
  * @ Author ：liuhao
  * @ Date   ：Created in 14:50 2018/7/27
  * @ 需求：
  * 自定义分区器的实现
  */
object SubjectCount_3 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setAppName("SubjectCount_1")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    // 获取文件中的数据
    val file: RDD[String] = sc.textFile("E:\\QF\\spark\\sparkcoursesinfomation\\spark\\data\\subjectaccess\\access.txt")

    // 模拟从数据库中获取学科信息并放到一个Array
    //    val subjects = Array("http://java.learn.com", "", "", "", "")

    // 切分
    val tupled: RDD[(String, Int)] = file.map(line => {
      val fields: Array[String] = line.split("\t")
      val url = fields(1)
      (url, 1)
    })

    // 将相同的url进行聚合, 得到各个学科的各个模块的访问量
    val summed: RDD[(String, Int)] = tupled.reduceByKey(_ + _)
    // 从聚合后的数据获取学科信息
    val subjectAndUrlAndCount: RDD[(String, (String, Int))] = summed.map(x => {
      val url: String = x._1
      val count = x._2
      // 获取学科
      val subject: String = new URL(url).getHost

      (subject, (url, count))
    }).cache()

    // 调用默认的分区器，partitionBy处理的是key/value
    // 用默认的分区器会发生数据倾斜，哈希碰撞
    // 使用自定义分区器解决该问题，一般解决到自己能承受的范围即可
    //    val partitioned: RDD[(String, (String, Int))] =
    //      subjectAndUrlAndCount.partitionBy(new HashPartitioner(3))

    // 获取所有的学科信息，一个学科信息对应一个分区，一个分区对应一个文件
  // keys获取所有的key
    // 因为要将学科信息传入自定义分区器，所以要collect

    val subjects: Array[String] =
    subjectAndUrlAndCount.keys.distinct().collect()

    // 调用自定义分区器
    val partitioner = new SubjectPartitioner(subjects)

    // 开始分区
    val partitioned: RDD[(String, (String, Int))] = subjectAndUrlAndCount.partitionBy(partitioner)

    // 生成每个学科的top3
    val res: RDD[(String, (String, Int))] =
      partitioned.mapPartitions(iter => {
        iter.toList.sortBy(_._2._2).reverse.take(3).iterator
      })

    // 开始存储
    res.saveAsTextFile("d:/out/20180728_1")

    // 指定保存的目录，好像只能是目录，不能是文件
//    partitioned.saveAsTextFile("")

    sc.stop()
  }

}

/**
  * 自定义分区器
  *
  * @param subjects
  */
class SubjectPartitioner(subjects: Array[String]) extends Partitioner {
  // 用来存储学科信息和分区号
  val subjectAndNum: mutable.HashMap[String, Int] = new mutable.HashMap[String, Int]()
  // 用来成成分区号的计数器
  var i = 0

  for (subject <- subjects) {
    subjectAndNum += (subject -> i)
    i += 1
  }

  // 获取分区数
  override def numPartitions: Int = subjects.length

  // 获取分区号
  override def getPartition(key: Any): Int =
    subjectAndNum.getOrElse(key.toString, 0)
}
