package com.sixth.group07.day02.RPCDemo

/**
  * @ Author ：liuhao
  * @ Date   ：Created in 15:21 2018/7/24
  * @ 
  */
trait RemoteMsg extends Serializable{

}

// 没有参数用case object
// 有参数用case class
// 在网络中传输的对象需要进行序列化
// Master -> self
case object CheckTimeOutWorker

// Worker -> Master
case class RegisterWorker(id:String, host:String, port:Int, memory:Int, cores:Int) extends RemoteMsg

// Master -> Worker
case class RegisteredWorker(masterUrl:String) extends RemoteMsg

// Worker -> Master
case class Heartbeat(WorkerId:String) extends RemoteMsg

// Worker -> self
case object SendHeartbeat