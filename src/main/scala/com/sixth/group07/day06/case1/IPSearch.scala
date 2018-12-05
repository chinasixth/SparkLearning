package com.sixth.group07.day06.case1

import java.sql.{Connection, Date, DriverManager, PreparedStatement}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ Author ：liuhao
  * @ Date   ：Created in 11:59 2018/7/30
  * @ 需求: 根据IP统计区域访问量
  * 实现思路：
  * 1.获取ip基础数据和用户点击流日志
  * 2.把用户的ip转换成long类型后，通过二分法查找该ip属于哪个ip段范围
  * 3.找到ip段后，再查找对应的省份
  * 4.聚合得到访问量
  * 5.把结果存储到mysql中
  *
  */
object IPSearch {


  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("IPSearch").setMaster("local")

    val sc: SparkContext = new SparkContext(conf)

    // 获取ip段基础数据
    val ipInfo: RDD[String] = sc.textFile("E:\\QF\\spark\\sparkcoursesinfomation\\spark\\data\\ipsearch\\ip.txt")
    // 切分ip基础数据
    val splittedIpInfo: RDD[(String, String, String)] = ipInfo.map(line => {
      val fields: Array[String] = line.split("\\|")
      val startIp = fields(2) // 起始ip
      val endIp = fields(3) // 结束ip
      val province = fields(6) // ip对应的省份

      (startIp, endIp, province)
    })

    // 在广播变量之前，需要把要广播的数据获取到(运行一个action的算子进行触发)
    val arrIpInfo: Array[(String, String, String)] = splittedIpInfo.collect()

    // 如果一个变量的值在executor端会经常调用的情况下，
    // 最好把该值事先广播到相应的executor端
    // 这样可以在计算的过程中减少网络IO，提高运行效率
    val broadcaseIpInfo: Broadcast[Array[(String, String, String)]] = sc.broadcast(arrIpInfo)

    // 获取用户点击流日志
    val userInfo = sc.textFile("E:\\QF\\spark\\sparkcoursesinfomation\\spark\\data\\ipsearch\\http.log")
    // 切分用户点击流日志，并根据二分查找得到该用户属于那个省份
    val provinceAndOne: RDD[(String, Int)] = userInfo.map(line => {
      val fields = line.split("\\|")
      val ip = fields(1) // 用户的ip
      val ip_long = ip2Long(ip) // 用户ip转换为Long类型
      // 获取到广播的ip基础信息
      val arrIpInfo: Array[(String, String, String)] = broadcaseIpInfo.value
      // 通过二分查找，确定用户的ip属于哪个ip段，返回ip段的索引
      val index = binarySearch(arrIpInfo, ip_long)
      // 根据索引查找对应的省份
      val province = arrIpInfo(index)._3

      (province, 1)
    })

    // 以省份开始聚合
    val reduced: RDD[(String, Int)] = provinceAndOne.reduceByKey(_ + _)

    //
    println(reduced.collect().toBuffer)

    // 存储到MySQL数据库
    // 一个分区对应一个链接，也有一行数据对应一个链接，
    reduced.foreachPartition(data2Mysql)

    sc.stop()

  }

  def ip2Long(ip: String): Long = {
    val fragments: Array[String] = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragments.length) {
      ipNum = fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }

  /**
    * 二分查找，根据ip，查找到ip属于哪个字段
    *
    * @param arrIpInfo
    * @param ip
    */
  def binarySearch(arrIpInfo: Array[(String, String, String)], ip: Long): Int = {
    var low = 0
    var high = arrIpInfo.length - 1

    while (low <= high) {
      val middle = (low + high) / 2
      if ((ip >= arrIpInfo(middle)._1.toLong && (ip <= arrIpInfo(middle)._2.toLong))) {
        return middle
      } else if (ip < arrIpInfo(middle)._1.toLong) {
        high = middle - 1
      } else {
        low = middle + 1
      }
    }
    -1
  }

  val data2Mysql = (it: Iterator[(String, Int)]) => {
    var conn: Connection = null
    var ps: PreparedStatement = null
    val sql = "insert into location_info(location, counts, access_date) " +
      "values (?,?,?)"
    val jdbcUrl = "jdbc:mysql://192.168.216.115:3306/bigdata?useUnicode=true&characterEncoding=utf8"
    val user = "root"
    val password = "123456"

    try {
      conn = DriverManager.getConnection(jdbcUrl, user, password)
      it.foreach(line => {
        ps = conn.prepareStatement(sql)
        ps.setString(1, line._1)
        ps.setInt(2, line._2)
        ps.setDate(3, new Date(System.currentTimeMillis()))

        ps.executeUpdate()
      })
    } catch {
      case e: Exception => println(e.printStackTrace())
    } finally {
      if (ps != null) {
        try {
          ps.close()
        } catch {
          case e: Exception => println(e.printStackTrace())
        }
      }
      if (conn != null) {
        try {
          conn.close()
        } catch {
          case e: Exception => println(e.printStackTrace())
        }
      }
    }
  }
}
