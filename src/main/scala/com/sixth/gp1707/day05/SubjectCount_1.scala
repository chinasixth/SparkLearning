package com.sixth.gp1707.day05

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ Author ：liuhao
  * @ Date   ：Created in 14:50 2018/7/27
  * @ 需求：
  * 统计所有用户对每个学科的各个模块的访问量，取top3
  * 实现思路：
  * 1.根据url先进行聚合--得到模块的访问量
  * 2.获取学科信息，
  * 3.根据学科进行分组
  * 4.根据学科进行组内排序
  * 5.取top3
  */

/*
* 由样例数据可知，数据有两个字段：访问时间和url，根据需求可知只有第二个字段对我们有用，所以切分并保留第二个字段
* 相同的url可能是有多个，也就是对每一个学科的相同模块访问次数有很多，
* 可以根据url将每一个学科的相同模块进行聚合，得到每一个学科对应模块的总访问量
* 最终取top3的时候是按照学科取得，而上一步得到的数据，相同学科的不同模块分散，所以要将学科切分出来作为key，
* 将相同学科的不同模块集合到一个value中，这样就能取出top3
* */
object SubjectCount_1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setAppName("SubjectCount_1")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)

    // 获取文件中的数据
    val file: RDD[String] = sc.textFile("E:\\QF\\spark\\sparkcoursesinfomation\\spark\\data\\subjectaccess\\access.txt")

    // 切分
    val tuped: RDD[(String, Int)] = file.map(line => {
      val fields: Array[String] = line.split("\t")
      val url = fields(1)
      (url, 1)
    })

    // 将相同的url进行聚合, 得到各个学科的各个模块的访问量
    val summed: RDD[(String, Int)] = tuped.reduceByKey(_ + _)

    // 从聚合后的数据获取学科信息
    val subjectAndUrlAndCount: RDD[(String, String, Int)] = summed.map(x => {
      val url: String = x._1
      val count = x._2
      // 获取学科
      val subject: String = new URL(url).getHost

      (subject, url, count)
    })

    // 根据学科进行分组
    val grouped: RDD[(String, Iterable[(String, String, Int)])] =
      subjectAndUrlAndCount.groupBy(_._1)

    // 去掉Iterable中的url
    //    val filtered: RDD[(String, Iterable[(String, Int)])] =
    // grouped.mapValues(_.map(x => (x._2, x._3)))

    // 在学科信息里，进行组内排序，按照模块的访问量进行降序排序
    val sorted: RDD[(String, List[(String, String, Int)])] =
      grouped.mapValues(_.toList.sortBy(_._3).reverse)
    //    val sorted: RDD[(String, List[(String, String, Int)])] =
    //    grouped.map((x => (x._1, x._2.toList.sortBy(_._3).reverse)))

    // 取top3
    val top3: RDD[(String, List[(String, String, Int)])] =
      sorted.mapValues(_.take(3))

    println(top3.collect.toBuffer)

    sc.stop()
  }

}
