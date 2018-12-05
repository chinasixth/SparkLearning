package com.sixth.group07.day12.work

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ Author ：liuhao
  * @ Date   ：Created in 9:10 2018/8/8
  * @ 找出包含单词最多的那一行有多少单词
  * 按行读取
  * 按行切分，不压平
  * 对每行中的单词进行去重
  * 统计每行的单词个数
  * 选择结果最大的行，直接选择不大好做，那么就排序呗，然后逆序，first，多带劲
  */
object SparkPractice_2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setAppName("SparkPractice_2")
      .setMaster("local")

    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile("E:\\QF\\spark\\day12\\testdata.txt")

    val lineWords: RDD[Array[String]] = lines.map(_.split(" "))

    val setted: RDD[Set[String]] = lineWords.map(x => x.toSet)

    val sized: RDD[(Set[String], Int)] = setted.map(s => (s, s.size))

    val tuple: (Set[String], Int) = sized.sortBy(_._2, false).first()

    println(tuple._1.toBuffer)
    println(tuple._2)

//    val wordNum: RDD[Int] = setted.map(_.size)
//
//    val max: Int = wordNum.max()

//    println(max)
    sc.stop()

  }
}
