package com.sixth.group07.day06.case1

import java.sql.{Date, DriverManager}

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ Author ：liuhao
  * @ Date   ：Created in 15:22 2018/7/30
  * @ 
  */
object JdbcRDDDemo {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("JdbcRDDDemo").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val url = "jdbc:mysql://192.168.216.115:3306/bigdata?useUnicode=true&characterEncoding=utf8"
    val user = "root"
    val password = "123456"

    val conn = () => {
      // 实例放在了上下文
      Class.forName("com.mysql.jdbc.Driver").newInstance()
      DriverManager.getConnection(url, user, password)
    }

    val sql = "select id,location,counts,access_date from location_info where id >= ? and id <= ? and location = '云南'"

    // lowerBound：指定查询的下界，当然还有上界
    // 指定分区数
    val jdbcRDD: JdbcRDD[(Int, String, Int, Date)] = new JdbcRDD(sc, conn, sql, 0, 500, 1,
      res => {
        val id = res.getInt("id")
        val location = res.getString("location")
        val counts = res.getInt("counts")
        val access_date = res.getDate("access_date")

        (id, location, counts, access_date)
      })

    println(jdbcRDD.collect().toBuffer)
  }
}
