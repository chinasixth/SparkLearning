package com.sixth.group07.day02.RPCDemo

import java.util.UUID

import akka.actor.{Actor, ActorRef, ActorSelection, ActorSystem, Props}
import com.typesafe.config.{Config, ConfigFactory}
import scala.concurrent.duration._

/**
  * @ Author ：liuhao
  * @ Date   ：Created in 15:06 2018/7/24
  * @ 
  */
class Worker(val host: String, val port: Int, val masterHost: String,
             val masterPort: Int, memory: Int, cores: Int)
  extends Actor {
  // 生成一个独一无二的worker id
  val workerId = UUID.randomUUID().toString
  // 用来存储MasterUrl
  var masterUrl: String = _
  // 心跳时间间隔
  val heartbeat_interval: Long = 10000
  // 用来存储Master的Actor
  var master: ActorSelection = _

  override def preStart(): Unit = {
    // 获取Master的Actor
    master = context.actorSelection(s"akka.tcp://${Master.MASTER_SYSTEM}" +
      s"@${masterHost}:${masterPort}/user/${Master.MASTER_ACTOR}")
    master ! RegisterWorker(workerId, host, port, memory, cores)
  }

  override def receive: Receive = {
    // Master响应过来的注册成功信息（MasterUrl）
    // 传url是因为高可用集群，master有可能宕掉，换到另一台机器上
    case RegisteredWorker(masterUrl) => {
      this.masterUrl = masterUrl
      // 启动一个发送心跳的定时器
      import context.dispatcher
      context.system.scheduler.schedule(0 millis, heartbeat_interval millis, self, SendHeartbeat)
    }
    case SendHeartbeat => {
      // 向Master发送注册信息
      master ! Heartbeat(workerId)
    }
  }
}

object Worker {
  val WORKER_SYSTEM = "WorkerSystem"
  val WORKER_ACTOR = "Worker"

  def main(args: Array[String]): Unit = {
    val host = args(0) //"127.0.0.1" // 本地回环地址，localhost
    val port = args(1).toInt //"8888"
    val masterHost = args(2)
    val masterPort = args(3).toInt
    val memory = args(4).toInt
    val cores = args(5).toInt

    val configStr =
      s"""
         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname = "$host"
         |akka.remote.netty.tcp.port = "$port"
      """.stripMargin

    // 封装创建actor的信息
    val config: Config = ConfigFactory.parseString(configStr)

    // 创建ActorSystem
    val actorSystem: ActorSystem = ActorSystem.create(WORKER_SYSTEM, config)
    // 隐式调用apply方法
    //    val actorSystem: ActorSystem = ActorSystem("WorkerSystem", config)

    // 创建Actor
    val worker: ActorRef = actorSystem.actorOf(Props(new Worker(host, port, masterHost, masterPort, memory, cores)), WORKER_ACTOR)

    // 线程等待
    actorSystem.awaitTermination()
  }
}
