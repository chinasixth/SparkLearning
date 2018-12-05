package com.sixth.group07.day02.akkademo

import akka.actor.{Actor, ActorRef, ActorSelection, ActorSystem, Props}
import com.typesafe.config.{Config, ConfigFactory}

/**
  * @ Author ：liuhao
  * @ Date   ：Created in 14:16 2018/7/24
  * @ 
  */
class Worker extends Actor{

  override def preStart(): Unit = {
    // 通过Master的url获取Master的actor，并发送信息
    val master: ActorSelection =
      context.actorSelection("akka.tcp://MasterSystem@127.0.0.1:6666/user/Master")
    master ! "connect"
  }

  override def receive: Receive ={
    case "self" => println("接收到自己给自己发送的信息")
    case "reply" => println("接收到Master发送过来的信息")
  }
}

object Worker{
  def main(args: Array[String]): Unit = {
    val host = "127.0.0.1" // 本地回环地址，localhost
    val port = "8888"
    val configStr =
      s"""
         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname = "$host"
         |akka.remote.netty.tcp.port = "$port"
      """.stripMargin

    // 封装创建actor的信息
    val config: Config = ConfigFactory.parseString(configStr)

    // 创建ActorSystem
    val actorSystem: ActorSystem = ActorSystem.create("WorkerSystem", config)
    // 隐式调用apply方法
//    val actorSystem: ActorSystem = ActorSystem("WorkerSystem", config)

    // 创建Actor
    val worker: ActorRef = actorSystem.actorOf(Props(new Worker), "Worker")

    Thread.sleep(1000)

    // 自己本类向自己的actor发送信息
    worker ! "self"
  }
}
