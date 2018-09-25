package TestSpark.subject

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ Author ：liuhao
  * @ Date   ：Created in 19:42 2018/9/24
  * @ 统计所有用户在某个学科某个模块的访问量，取top3（模块的top）
  */
object Subject1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("src/main/data/subjectaccess/access.txt")
    val subInfo: RDD[(String, Int)] = lines.map(line => {
      val fields: Array[String] = line.split("\t")
      val url = fields(1)
      (url, 1)
    })
    val aggregated: RDD[(String, Int)] = subInfo.aggregateByKey(0)(_+_, _+_)
    val host: RDD[(String, String, Int)] = aggregated.map(info => {
      val url = new URL(info._1)
      val host = url.getHost
      (host, info._1, info._2)
    })
    val sorted: RDD[(String, List[(String, String, Int)])] = host.groupBy(_._1).mapValues(_.toList.sortBy(_._3).reverse)
    val taked: RDD[List[(String, String, Int)]] = sorted.mapValues(_.take(2)).map(_._2)
    taked.collect.foreach(println)

    sc.stop()
  }
}
