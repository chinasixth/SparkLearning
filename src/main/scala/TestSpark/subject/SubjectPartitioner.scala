package TestSpark.subject

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

/**
  * @ Author ：liuhao
  * @ Date   ：Created in 20:46 2018/9/24
  * @ 
  */
object SubjectPartitioner {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("src/main/data/subjectaccess/access.txt")
    val urlInfo: RDD[(String, Int)] = lines.map(line => {
      val fields = line.split("\t")
      val url = fields(1)
      (url, 1)
    })
    val reduced: RDD[(String, Int)] = urlInfo.reduceByKey(_ + _)
    val persisted: RDD[(String, Int)] = reduced.persist()

    println(persisted.partitions.length)

    val hosted: RDD[(String, String, Int)] = persisted.map(x => {
      val url = new URL(x._1)
      val host = url.getHost
      (host, x._1, x._2)
    })
    val cached: RDD[(String, Iterable[(String, String, Int)])] = hosted.groupBy(_._1).cache()

    val subjects: Array[String] = cached.keys.distinct().collect()
    val subPartition = new SubjectPartitione(subjects)

    val partitioned: RDD[(String, Iterable[(String, String, Int)])] = cached.partitionBy(subPartition)

    val sorted: RDD[(String, List[(String, String, Int)])] = partitioned.mapValues(x => x.toList.sortBy(_._3).reverse.take(2))
    println(sorted.partitions.length)

    sc.stop()
  }

}

class SubjectPartitione(subjects: Array[String]) extends Partitioner {
  val subMap = new mutable.HashMap[String, Int]()
  var i = 0

  for (sub <- subjects) {
    subMap += (sub -> i)
    i += 1
  }

  override def numPartitions: Int = subjects.length

  override def getPartition(key: Any): Int = {
    subMap.getOrElse(key.toString, 0)
  }
}
