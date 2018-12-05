package com.sixth.group07.day14

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
// 这个是手动导入的
import org.elasticsearch.spark._

/**
  * @ Author ：liuhao
  * @ Date   ：Created in 16:20 2018/8/9
  * @ 
  */
object ESRDDDemo {
  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf()
//      .setAppName("ESRDDDemo")
//      .setMaster("local")
//      .set("es.nodes", "localhost")
//      .set("es.port", "9200")
//      .set("es.index.auto.create", "true")
//
//    val sc = new SparkContext(conf)
//
//    // 用于查询es的查询语句
//    //    val query = "{\"query\":{\"match_all\":{}}}"
//    val query =
//    """
//        {"query":{"match_all":{}}}
//      """.stripMargin
//
//    // 第一个参数指定index
//    // 返回的是一个key/value的数据，其中key是id，value是id对应的那条数据
//    val queryRDD: RDD[(String, collection.Map[String, AnyRef])] = sc.esRDD("blog", query)
//    // id对应的数据，其中String对应的是字段名称，AnyRef是字段对应的值
//    val valueRDD: RDD[collection.Map[String, AnyRef]] = queryRDD.map(_._2)
//    // 把每个字段的值取出来
//    val fieldValue: RDD[(Any, AnyRef, AnyRef)] = valueRDD.map(line => {
//      val id = line.getOrElse("id", -1)
//      val title = line.getOrElse("title", null)
//      val content = line.getOrElse("content", null)
//
//      (id, title, content)
//    })
//
//    println(fieldValue.collect.toBuffer)
//    sc.stop()
  }
}











