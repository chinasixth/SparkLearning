package com.sixth.gp1707.day02.akkademo

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import com.typesafe.config.{Config, ConfigFactory}

/**
  * @ Author ：liuhao
  * @ Date   ：Created in 14:15 2018/7/24
  * @ 
  */
class Master extends Actor {
  // 声明周期方法，只执行一次，用作初始化
  override def preStart(): Unit = {
    println("preStart方法被执行...")
  }

  // 在preStart方法之后执行，会不断执行
  override def receive: Receive = {
    case "start" => println("接收到自己发送的信息")
    case "stop" => println("stop")
    case "connect" => {
      println("connect")
      sender ! "reply"
    }
  }
}

object Master{
  def main(args: Array[String]): Unit = {
    val host = "127.0.0.1" // 本地回环地址，localhost
    val port = "6666"
    val configStr: String =
      s"""
        |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
        |akka.remote.netty.tcp.hostname = "$host"
        |akka.remote.netty.tcp.port = "$port"
      """.stripMargin

    // 封装创建actor的信息
    val config: Config = ConfigFactory.parseString(configStr)

    // 创建ActorSystem
    val actorSystem: ActorSystem = ActorSystem.create("MasterSystem", config)

    // 创建Actor
    val master: ActorRef = actorSystem.actorOf(Props[Master], "Master")

    // 自己本类向自己的actor发送信息
    master ! "start"

  }
}
