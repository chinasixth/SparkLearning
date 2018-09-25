package TestSpark.sql

import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ Author ：liuhao
  * @ Date   ：Created in 8:07 2018/9/25
  * @ 
  */
object SQLInferring {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val lines = sc.textFile("src/main/data/girl.txt")
    val info: RDD[(String, String, String)] = lines.map(line => {
      val fields = line.split(" ")
      val name = fields(0)
      val fv = fields(1)
      val age = fields(2)
      (name, fv, age)
    })

    val infoSchema: RDD[Info] = info.map(x => Info(x._1, x._2.toInt, x._3.toInt))
    import sqlContext.implicits._
    val df: DataFrame = infoSchema.toDF()
    df.registerTempTable("tmp")
    val sDF: DataFrame = sqlContext.sql("select * from tmp")
    val url = "jdbc:mysql://hadoop05:3306/spark"
    val table = "test"
    val properties = new Properties()
    properties.put("username", "root")
    properties.put("password", "123456")
    properties.put("driver", "com.mysql.jdbc.Driver")

    sDF.write.mode(SaveMode.Append).jdbc(url, table, properties)

    sc.stop()
  }

}
case class Info(name: String, fv: Int, age: Int)
