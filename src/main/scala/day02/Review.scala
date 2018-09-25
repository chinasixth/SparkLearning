package day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ Author ：liuhao
  * @ Date   ：Created in 14:30 2018/9/19
  * @ 
  */
object Review {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("review")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    // new ParallelCollectionRDD[T]
     val rdd: RDD[Int] = sc.parallelize(List(5,6,4,7,3,8,2,9,1,10))
    // val sorted: RDD[Int] = rdd.map(_*2).sortBy(x => x)
    // val filtered = sorted.filter(_ >= 10)

//    val rdd1 = sc.parallelize(Array("a b c", "d e f", "h i j"))
//    val flatMap: RDD[String] = rdd1.flatMap(_.split(" "))

//    val rdd1 = sc.parallelize(List(List("a b c", "a b b"), List("e f g", "a f g"), List("h i j", "a a b")))
//    val flatMap = rdd1.flatMap(_.flatMap(_.split(" ")))

//    val rdd1 = sc.parallelize(List(5,6,4,3))
//    val rdd2 = sc.parallelize(List(1,2,3,4))
//    val union: RDD[Int] = rdd1.union(rdd2)
//    val intersection: RDD[Int] = rdd1.intersection(rdd2)
//    val distinct = union.distinct()
//    val rdd1 = sc.parallelize(List(("tom",1),("jerry",3),("kitty",2)))
//    val rdd2 = sc.parallelize(List(("jerry",2),("tom",1),("shuke",2)))
//    val innerJoin: RDD[(String, (Int, Int))] = rdd1.join(rdd2)
//    val leftJoin: RDD[(String, (Int, Option[Int]))] = rdd1.leftOuterJoin(rdd2)
//    val rightJoin: RDD[(String, (Option[Int], Int))] = rdd1.rightOuterJoin(rdd2)
//    val union: RDD[(String, Int)] = rdd1.union(rdd2)
//    val groupedByKey: RDD[(String, Iterable[Int])] = union.groupByKey()
//    val sum: RDD[(String, Int)] = union.groupByKey().map(x => (x._1, x._2.toList.sum))
//    val reduce: RDD[(String, Int)] = union.reduceByKey(_+_)

//    val rdd1 = sc.parallelize(List(("tom", 1), ("tom", 2), ("jerry", 3), ("kitty", 2)))
//    val rdd2 = sc.parallelize(List(("jerry", 2), ("tom", 1), ("shuke", 2)))
//    val cogroup: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd1.cogroup(rdd2)

//    print(groupedByKey.collect.toBuffer)

//    val rdd1 = sc.parallelize(List(1,2,3,4,5,6),2)
//    val aggregate: Int = rdd1.aggregate(100)(_+_, _+_)
//    print(aggregate)


//    val rdd1 = sc.parallelize(List("a","b","c","d","e","f","g"),1)
//    val aggregate: String = rdd1.aggregate("=")(_+_, _+_)
//    print(aggregate)



//    val arr = List("a","b","c","d","e","f","g")
//    val a: String = arr.aggregate("=")(_+_, _+_)
//    print(a)

//    val rdd4 = sc.parallelize(List("dog","cat","gnu","salmon","rabbit","turkey","wolf","bear","bee"), 3)
//    val rdd5 = sc.parallelize(List(1,1,2,2,2,1,2,2,2), 3)
//    val rdd6 = rdd5.zip(rdd4)
//    val rdd7 = rdd6.combineByKey(List(_), (x: List[String], y: String) => x :+ y, (m: List[String], n: List[String]) => m++n)

//    val rdd1 = sc.parallelize(List(("a", 1), ("b", 2), ("b", 2), ("c", 2), ("c", 1)))
//    val countByKey: collection.Map[String, Long] = rdd1.countByKey()
//    val countByValue: collection.Map[(String, Int), Long] = rdd1.countByValue()
//    println(countByKey)
//    print(countByValue)

//    val rdd1 = sc.parallelize(List(("e", 5), ("c", 3), ("d", 4), ("c", 2), ("a", 1)))
//    val rdd2 = rdd1.filterByRange("c", "d")
//    print(rdd2.collect.toBuffer)

//    val rdd = sc.parallelize(List(("a", 1), ("b", 2)))
//    val value: RDD[Unit] = rdd.map(print(_))
//    print(value.collect.toBuffer)
//    println(rdd.collectAsMap())
//    println(rdd.collect.toBuffer)


    sc.stop()
  }
}
