//package com.qf.gp1707.day01
//
//import scala.actors.{Actor, Future}
//import scala.collection.mutable.ArrayBuffer
//import scala.io.Source
//
///**
//  * @ Author ：liuhao
//  * @ Date   ：Created in 20:46 2018/7/23
//  * @
//  */
//object ActorWC {
//  def main(args: Array[String]): Unit = {
//    val replys = new ArrayBuffer[Future[Any]]()
//    val filterReplys: ArrayBuffer[Map[String, Int]] = new ArrayBuffer[Map[String, Int]]()
//
//    val files = Array("", "", "")
//
//    for (file <- files) {
//      val task: Task = new Task
//      task.start()
//
//      val reply: Future[Any] = task !! SmTask(file)
//      println(reply)
//      replys += reply
//    }
//
//    while (replys.size > 0) {
//      val dones: ArrayBuffer[Future[Any]] = replys.filter(_.isSet)
//
//      for (done <- dones) {
//        val mapped: Map[String, Int] = done.apply().asInstanceOf[Map[String, Int]]
//
//        filterReplys += mapped
//        dones -= done
//      }
//    }
//
//  }
//}
//
//class Task extends Actor {
//  override def act(): Unit = {
//    // 当线程启动的时候，根据接收的信息，执行任务
//    receive {
//      case SmTask(file) => {
//        val linesIt: Iterator[String] = Source.fromFile(file).getLines()
//        val sum: Map[String, Int] = linesIt.toList.flatMap(_.split(" ")).map((_, 1)).groupBy(_._1).mapValues(_.size)
//        // 异步发送数据，不需要返回值
//        sender ! sum
//      }
//    }
//  }
//}
//
//case class SmTask(file: String)