package TestSpark.mydefine

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ Author ：liuhao
  * @ Date   ：Created in 15:42 2018/9/24
  * @ 
  */
object OrderedImplicit {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("src/main/data/girl.txt")
    val girlInfo: RDD[(String, String, String)] = lines.map(line => {
      val fields: Array[String] = line.split(" ")
      val name = fields(0)
      val fv = fields(1)
      val age = fields(2)
      (name, fv, age)
    })

    import MyPredef.selectOrdered

    val sorted: RDD[(String, String, String)] = girlInfo.sortBy(info => Girl(info._2.toInt, info._3.toInt), false)

    sorted.collect.foreach(println)
    sc.stop()
  }

}
case class Girl(fv:Int, age:Int)
