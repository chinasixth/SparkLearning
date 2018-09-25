package TestSpark.subject

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ Author ：liuhao
  * @ Date   ：Created in 20:24 2018/9/24
  * @ 
  */
object SubjectPersist {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("src/main/data/subjectaccess/access.txt")
    val urlInfo: RDD[(String, Int)] = lines.map(line => {
      val fields: Array[String] = line.split("\t")
      val url = fields(1)
      (url, 1)
    })
    val reduced: RDD[(String, Int)] = urlInfo.reduceByKey(_+_)

    // 持久化操作
    val persisted: RDD[(String, Int)] = reduced.persist()

    val hosted: RDD[(String, String, Int)] = persisted.map(x => {
      val url = new URL(x._1)
      val host = url.getHost
      (host, x._1, x._2)
    })
    val grouped: RDD[(String, Iterable[(String, String, Int)])] = hosted.groupBy(_._1)
    val sorted: RDD[(String, List[(String, String, Int)])] = grouped.mapValues(it => it.toList.sortBy(_._3).reverse)
    val result: RDD[List[(String, String, Int)]] = sorted.mapValues(_.take(2)).map(_._2)
    result.collect.foreach(println)

    sc.stop()
  }

}
