package TestSpark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ Author ：liuhao
  * @ Date   ：Created in 10:44 2018/9/24
  * @ 
  */
object MobileLocated {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("MobileLocated")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    val files = sc.textFile("src/main/data/log")
    val lacAndPhoneAndTime: RDD[((String, String), Long)] = files.map(line => {
      val fields: Array[String] = line.split(",")
      val phone = fields(0)
      val lac = fields(2)
      val event = fields(3)
      val time = if (event.equals("0")) fields(1).toLong else -(fields(1).toLong)
      ((lac, phone), time)
    })
    //    val grouped: RDD[((String, String), Iterable[Long])] = LacAndPhoneAndTime.groupByKey()
    //    val sumed: RDD[((String, String), Long)] = grouped.mapValues(_.toList.sum)

    val aggregated: RDD[((String, String), Long)] = lacAndPhoneAndTime.aggregateByKey(0L)(_ + _, _ + _)

    val lacAsKey: RDD[(String, (String, Long))] = aggregated.map(x => {
      val lac = x._1._1
      val phone = x._1._2
      val timeLength = x._2
      (lac, (phone, timeLength))
    })

    val lacInfo: RDD[String] = sc.textFile("src/main/data/lac_info.txt")
    val lacAndXY: RDD[(String, (String, String))] = lacInfo.map(line => {
      val fields: Array[String] = line.split(",")
      val lac = fields(0)
      val lac_X = fields(1)
      val lac_Y = fields(2)
      (lac, (lac_X, lac_Y))
    })

    val joined: RDD[(String, ((String, Long), (String, String)))] = lacAsKey join lacAndXY
    val phoneAndTimeAndXY: RDD[(String, Long, (String, String))] = joined.map(x => {
      val phone = x._2._1._1
      val time = x._2._1._2
      val XY = x._2._2
      (phone, time, XY)
    })
    val grouped: RDD[(String, Iterable[(String, Long, (String, String))])] = phoneAndTimeAndXY.groupBy(_._1)
    val sorted: RDD[(String, List[(String, Long, (String, String))])] = grouped.mapValues(value => {
      value.toList.sortBy(_._2).reverse
    })

    val take: RDD[(String, List[(String, Long, (String, String))])] = sorted.mapValues(_.take(2))

    take.map(_._2).collect().foreach((x: List[(String, Long, (String, String))]) => {
      x.foreach(println)
      println("---------------")
    })

    sc.stop()
  }

}
