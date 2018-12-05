//package com.qf.gp1707.day01
//
//import scala.actors.{Actor, Future}
//import scala.io.Source
//
///**
//  * @ Author ：liuhao
//  * @ Date   ：Created in 8:19 2018/7/24
//  * @
//  */
//object ActorData {
//  def main(args: Array[String]): Unit = {
//    val testActor: Test = new Test
//    testActor.start()
//
//    val reply: Future[Any] = testActor !! SubmitTask("e:/hadoopdata/123.txt")
//    Thread.sleep(1000)
//    val map: Map[String, Int] = reply.apply().asInstanceOf[Map[String, Int]]
//    println(map)
//  }
//
//}
//
//class Test extends Actor{
//  override def act(): Unit ={
//    receive {
//      case SubmitTask(file) =>{
//        val lines: Iterator[String] = Source.fromFile(file).getLines()
//        val map: Map[String, Int] = lines.toList.flatMap(_.split(" ")).map((_, 1)).groupBy(_._1).mapValues(_.size)
//        println(map)
//        sender ! map
//      }
//    }
//  }
//}
//
//case class SubmitTask(str:String)
