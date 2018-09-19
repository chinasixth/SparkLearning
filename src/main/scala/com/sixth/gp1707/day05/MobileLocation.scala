package com.sixth.gp1707.day05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ Author ：liuhao
  * @ Date   ：Created in 11:23 2018/7/27
  * @ 需求：
  * 在一定时间范围内，求用户在所有基站（lac，位置区码）停留的时长，再取top2
  * 要求输出结果的格式是；（phone, ()）
  * 1.用户在每一个基站停留的总时长
  * 2.把经纬度信息join进来
  * 3.按照手机号进行分组
  * 4.组内排序，并取top2
  */
/*
* 详细分析和实现步骤：
* 根据需求，需要先将用户在所有基站停留的时间求出来，然后进行排序
* 由log日志文件中的字段可知：进出基站的时间都是成对存在的，不会出现只进不出和只出不进
* 首先是创建SparkConf，根据conf来创建SparkContext，这是spark集群的入口类，必不可少
* 首先按行读取每一个文件，因为文件的字段类型是一样的，所以可以将文件内容读取到一个RDD中
* 按行切分每一个字段，在这里要考虑到下一步，因为我们要计算用户在每一个基站停留的时间，所以会进行分组操作，
* 在切分完字段以后，将用户和lacID作为key，时间戳作为value，这样在分组的时候就可以使用value将我们需要的停留时间拿出来
* 这里要注意：虽然在数据中进入基站和离开基站是成对出现的，但是不能计算得出在基站停留的每一个时间段，求出的是在基站停留的总时长
* 切分完以后的数据是((phone, lacId), time)，time的真实值是根据基站的0/1字段来设置的
* 根据切分以后的数据中的key，将value进行聚合，就得到用户在每一个基站停留的总时长
* 最终的结果需要将经纬度打印出来，所以还要根据基站的lacId和lacInfo这个文件进行join
* 因为要join，分析可知需要使用lac为key进行join，所以要将刚刚聚合后的结果对字段进行整理
* 然后读取lacInfo文件中的内容，同样是将lac作为key，然后对join以后的数据进行字段整理
* 因为一个用户可能在多个基站中停留，所以join以后用户会对应多个基站，可以根据用户进行聚合，
* 将用户所停留的所有的基站信息放入一个value，方便我们取出top2
*
* */
object MobileLocation {
  def main(args: Array[String]): Unit = {
    // 模板代码
    val conf: SparkConf = new SparkConf()
      .setAppName("MobileLocation")
      .setMaster("local[2]")

    val sc: SparkContext = new SparkContext(conf)

    // 获取用户访问的基站信息
    // 双反斜杠在集群中不识别
    val files: RDD[String] = sc.textFile("E://QF/spark/sparkcoursesinfomation/spark/data/lacduration/log")

    // 切分用户
    val phoneAndLacAndTime: RDD[((String, String), Long)] = files.map(line => {
      // 切分每一行的数据
      val fields: Array[String] = line.split(",")
      // 用户手机号
      val phone: String = fields(0)
      // 时间戳，这里其实是数据
      val time: Long = fields(1).toLong
      // 基站id
      val lac: String = fields(2)
      // 事件类型
      val eventType = fields(3)
      // 根据时间类型设置时间戳的最终值
      val time_long = if (eventType.equals("0")) time else -time
      // 因为要根据用户所在的基站求出停留时间，分析可得，需要将用户和基站作为key
      ((phone, lac), time_long)

    })

    // 用户在相同的基站
    val summedPhoneAndLacAndTime: RDD[((String, String), Long)] =
      phoneAndLacAndTime.reduceByKey(_ + _)

    // 为了便于和经纬度进行join，需要把lac字段进行放到key的位置
    val lacAndPhoneAndTime: RDD[(String, (String, Long))] =
      summedPhoneAndLacAndTime.map(line => {
        // 拿到手机号
        val phone = line._1._1
        // 获取基站id
        val lac = line._1._2
        // 用户在该基站停留的总时长
        val time: Long = line._2

        (lac, (phone, time))
      })

    // 获取基站的基础信息
    val lacInfo: RDD[String] = sc.textFile("E:\\QF\\spark\\sparkcoursesinfomation\\spark\\data\\lacduration\\lac_info.txt")

    // 切分基站基础信息
    val lacAndXY: RDD[(String, (String, String))] = lacInfo.map(line => {
      val fields: Array[String] = line.split(",")
      val lac: String = fields(0)
      // 经度
      val x: String = fields(1)
      // 维度
      val y: String = fields(2)
      // 以lac为key，方便join
      (lac, (x, y))
    })

    // 把经纬度信息join到用户访问信息
    val joined: RDD[(String, ((String, Long), (String, String)))] =
      lacAndPhoneAndTime join lacAndXY

    println(joined.collect().toBuffer)

    // 为了方便计算，要把数据整合
    val phoneAndTimeAndXY: RDD[(String, Long, (String, String))] =
      joined.map((item: (String, ((String, Long), (String, String)))) => {
        val phone: String = item._2._1._1 // 手机号
        //        val lac = item._1 // 获取基站id
        val time = item._2._1._2 //停留时长
        val xy = item._2._2 // 经纬度

        (phone, time, xy)
      })

    // 对用户进行分组，并进行组内排序，按照在基站停留的时长
    // 按照用户的手机号进行分组
    val grouped: RDD[(String, Iterable[(String, Long, (String, String))])] =
      phoneAndTimeAndXY.groupBy(_._1)

    // 按照时长进行降序排序
    val sorted: RDD[(String, List[(String, Long, (String, String))])] =
      grouped.mapValues(_.toList.sortBy(_._2).reverse)

    // 获取top2
    val top2: RDD[(String, List[(String, Long, (String, String))])] =
      sorted.mapValues(_.take(2))

    // 打印top2
    println(top2.collect.toBuffer)

    // 关闭
    sc.stop()
  }
}
