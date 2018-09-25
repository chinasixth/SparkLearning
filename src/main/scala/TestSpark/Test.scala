package TestSpark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ Author ：liuhao
  * @ Date   ：Created in 14:36 2018/9/21
  * @ 
  */
object Test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    val name = Array((1, "spark"), (2, "flink"), (3, "hadoop"))
    val score = Array((1, 100), (2, 90), (3, 80))

    val nameRDD = sc.parallelize(name)
    val scoreRDD = sc.parallelize(score)
    
    val joined = nameRDD.join(scoreRDD)
    joined.collect.foreach(println)
    sc.stop()
  }

}
