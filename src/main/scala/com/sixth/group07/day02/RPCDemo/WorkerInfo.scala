package com.sixth.group07.day02.RPCDemo

/**
  * @ Author ：liuhao
  * @ Date   ：Created in 15:12 2018/7/24
  * @ 
  */
class WorkerInfo(val id:String, val host:String,
                 val port:Int, val memory:Int, val cores:Int) {
  // 用来存储最后一次心跳时间
  var lastHeartbeatTime:Long =_
}
