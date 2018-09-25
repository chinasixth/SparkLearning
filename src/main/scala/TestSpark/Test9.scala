package TestSpark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ Author ：liuhao
  * @ Date   ：Created in 15:03 2018/9/24
  * @ 
  */
object Test9 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("src/main/data/test.txt")
    val idAndTimeAndWeb: RDD[(String, String, String)] = lines.map(x => {
      val fields: Array[String] = x.split(" ")
      val id = fields(0)
      val time = fields(1)
      val web = fields(2)
      (id, time, web)
    })
    val grouped = idAndTimeAndWeb.groupBy(_._1)
    // 先按key分组，然后使用mapValues进行组内排序，每次操作的是一个key对应的value
    val sorted: RDD[(String, List[(String, String, String)])] = grouped.mapValues(x => {
      x.toList.sortBy(_._2)
    })

//    sorted.map(_._2).collect.foreach(x => {
//      x.foreach(println)
//      println("---------------")
//    })

    val sortedBykey: RDD[(String, List[(String, String, String)])] = sorted.sortBy(_._1)
    sortedBykey.map(_._2).collect.foreach(it => {
      it.foreach(println)
      println("---------------")
    })
    sc.stop()
  }
}
