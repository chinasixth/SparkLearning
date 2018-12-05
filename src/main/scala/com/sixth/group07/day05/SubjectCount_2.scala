package com.sixth.group07.day05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ Author ：liuhao
  * @ Date   ：Created in 14:50 2018/7/27
  * @ 需求：
  * 统计所有用户对每个学科的各个模块的访问量，取top3
  * RDD的持久化，cache缓存
  */
object SubjectCount_2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setAppName("SubjectCount_1")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    // 获取文件中的数据
    val file: RDD[String] = sc.textFile("E:\\QF\\spark\\sparkcoursesinfomation\\spark\\data\\subjectaccess\\access.txt")

    // 模拟从数据库中获取学科信息并放到一个Array
    val subjects = Array("http://java.learn.com",
      "http://ui.learn.com",
      "http://bigdata.learn.com",
      "http://android.learn.com",
      "http://h5.learn.com")

    // 切分
    val tupled: RDD[(String, Int)] = file.map(line => {
      val fields: Array[String] = line.split("\t")
      val url = fields(1)
      (url, 1)
    })

    // 将相同的url进行聚合, 得到各个学科的各个模块的访问量
    val summed: RDD[(String, Int)] = tupled.reduceByKey(_ + _)

    // 在发生shuffle后的数据一般都比较重要，通常会将shuffle后的数据进行持久化（cache）
    // 优点：便于以后再次获取该数据时，可以直接从缓存中读取，提高运算效率
    // 缓存时考虑的点：
    // 1.如果shuffle后的数据会被经常用到，适合缓存
    // 2.在缓存时，需要结合物理资源（内存）来考虑缓存的地方，如果内存比较吃紧，则可以缓存到磁盘

    // 调用cache默认存储到磁盘
    val cached: RDD[(String, Int)] = summed.cache()
    // 存储到磁盘，需要传入一个缓存级别，默认还是存储到内存
    //    summed.persist(StorageLevel.DISK_ONLY)

    for (subject <- subjects) {
      val filtered: RDD[(String, Int)] =
        cached.filter(_._1.startsWith(subject))
      val top3: Array[(String, Int)] = filtered.sortBy(_._2, false).take(3)
      println(top3.toBuffer)
//      sc.parallelize(top3).saveAsTextFile("d://out/")
    }

    //    println(top3.collect.toBuffer)

    sc.stop()
  }
}
