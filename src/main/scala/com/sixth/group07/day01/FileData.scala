package com.sixth.group07.day01

import scala.io.{BufferedSource, Source}

/**
  * @ Author ：liuhao
  * @ Date   ：Created in 20:57 2018/7/23
  * @ 
  */
object FileData {
  def main(args: Array[String]): Unit = {
    val file = "E:\\hadoopdata\\123.txt"

    val fileSource: BufferedSource = Source.fromFile(file)
    val linesIt: Iterator[String] = fileSource.getLines()
    val lines: List[String] = linesIt.toList
    val wordsPre: List[String] = lines.flatMap(_.split(" "))
    val words: List[String] = wordsPre.filter(_ != "")
    val tuples: List[(String, Int)] = words.map((_,1))
    val grouped: Map[String, List[(String, Int)]] = tuples.groupBy(_._1)
    val summed: Map[String, Int] = grouped.mapValues(_.size)
    
    println(summed)
  }

}
