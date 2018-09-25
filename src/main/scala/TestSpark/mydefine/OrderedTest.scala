//package TestSpark.mydefine
//
//import com.sixth.gp1707.day06.Girl
//import org.apache.spark.rdd.RDD
//import org.apache.spark.{SparkConf, SparkContext}
//
///**
//  * @ Author ：liuhao
//  * @ Date   ：Created in 15:21 2018/9/24
//  * @
//  */
//object OrderedTest {
//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf()
//      .setAppName(this.getClass.getName)
//      .setMaster("local[*]")
//    val sc = new SparkContext(conf)
//
//    val lines = sc.textFile("src/main/data/girl.txt")
//    val info: RDD[(String, String, String)] = lines.map(line => {
//      val fields: Array[String] = line.split(" ")
//      val name = fields(0)
//      val fv = fields(1)
//      val age = fields(2)
//      (name, fv, age)
//    })
//    val sorted: RDD[(String, String, String)] = info.sortBy(girl => {
//      Girl(girl._2.toInt, girl._3.toInt)
//    }, false)
//    sorted.collect.foreach(println)
//
//    sc.stop()
//
//  }
//}
//
//case class Girl(fv: Int, age: Int) extends Ordered[Girl] {
//  override def compare(that: Girl): Int = {
//    if (this.fv == that.fv)
//      that.age - this.age
//    else
//      this.fv - that.fv
//  }
//}