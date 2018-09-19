package com.sixth.gp1707.day06

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ Author ：liuhao
  * @ Date   ：Created in 16:11 2018/7/30
  * @ 
  */
object CustomSort {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("CustomSort").setMaster("local")

    val sc = new SparkContext(conf)

    val girlInfo: RDD[(String, Int, Int)] = sc.parallelize(Array(("xuaner", 100, 16),("tuoer",100,18),("langer", 90, 20)))

    // 方式一,使用饮食转换
//    import MyPredef.girlOrdering
//
//    val res: RDD[(String, Int, Int)] = girlInfo.sortBy(goddess => Girl(goddess._2, goddess._3), false)
//    println(res.collect.toBuffer)

    // 方式二,不使用隐式转换
    val res: RDD[(String, Int, Int)] = girlInfo.sortBy(goddess => Girl(goddess._2, goddess._3), false)
    println(res.collect.toBuffer)

    sc.stop()
  }
}

// 第一种方式
//case class Girl(fv:Int, age:Int)

// 第二种方式
case class Girl(fv:Int, age: Int) extends Ordered[Girl] {
  override def compare(that: Girl): Int = {
    if (that.fv == that.fv) {
      that.age - this.age
    } else {
      this.fv - that.fv
    }
  }
}