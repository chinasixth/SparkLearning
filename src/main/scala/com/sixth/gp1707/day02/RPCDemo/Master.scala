package com.sixth.gp1707.day02.RPCDemo

// ctrl + alt + O，处理一些没有用的包
import akka.actor.{Actor, ActorSystem, Props}
import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.mutable
import scala.concurrent.duration._

/**
  * @ Author ：liuhao
  * @ Date   ：Created in 15:06 2018/7/24
  * @ 
  */
class Master(val masterHost: String, val masterPort: Int) extends Actor {
  // 用来存储Worker的注册信息
  val idToWorker = mutable.HashMap[String, WorkerInfo]()

  // 用来存储Worker的信息，HashSet可以自动去重
  val workers = new mutable.HashSet[WorkerInfo]()

  // Worker的超时时间间隔
  val checkInterval: Long = 15000

  override def preStart(): Unit = {
    // 启动一个定时器，定时检查超时的Worker
    import context.dispatcher
    // self 表示启动以后先向自己发送信息
    context.system.scheduler.schedule(0 millis, checkInterval millis, self, CheckTimeOutWorker)
  }

  override def receive: Receive = {
    // Worker向Master发送注册信息
    case RegisterWorker(id, host, port, memory, cores) => {
      if (!idToWorker.contains(id)) {
        val workerInfo = new WorkerInfo(id, host, port, memory, cores)
        idToWorker += (id -> workerInfo)
        workers += workerInfo

        println("a worker registered")

        sender ! RegisteredWorker(s"akka.tcp://${Master.MASTER_SYSTEM}" +
          s"@${masterHost}:${masterPort}/user/${Master.MASTER_ACTOR}")
      }
    }
    case Heartbeat(workerId) => {
      // 通过传输过来的workerId获取对应的workerInfo
      val workerInfo = idToWorker(workerId)
      // 获取当前时间
      val currentTime: Long = System.currentTimeMillis()
      // 更新最后一次心跳时间
      workerInfo.lastHeartbeatTime = currentTime
    }
    case CheckTimeOutWorker => {
      val currentTime = System.currentTimeMillis()
      // 获取超时的Worker
      val toRemove: mutable.HashSet[WorkerInfo] = workers.filter(w => currentTime - w.lastHeartbeatTime > checkInterval)
      // 以处超时的worker
      toRemove.foreach(deadWorker => {
        idToWorker -= deadWorker.id
        workers -= deadWorker
      })

      println(s"num of workers:${workers.size}")
    }
  }
}

object Master {
  val MASTER_SYSTEM = "MasterSystem"
  val MASTER_ACTOR = "Master"

  def main(args: Array[String]): Unit = {
    val host = args(0) //"127.0.0.1" 本地回环地址，localhost
    val port = args(1).toInt //"6666"
    val configStr =
      s"""
         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname = "$host"
         |akka.remote.netty.tcp.port = "$port"
      """.stripMargin

    // 封装创建actor的信息
    val config: Config = ConfigFactory.parseString(configStr)

    // 创建ActorSystem
    val actorSystem: ActorSystem = ActorSystem.create(MASTER_SYSTEM, config)

    // 创建Actor
    actorSystem.actorOf(Props(new Master(host, port)), MASTER_ACTOR)
  }
}