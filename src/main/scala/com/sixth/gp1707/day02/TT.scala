package com.sixth.gp1707.day02

import java.net.URL

import com.typesafe.config.{Config, ConfigFactory}

/**
  * @ Author ：liuhao
  * @ Date   ：Created in 19:26 2018/7/24
  * @ 
  */
object TT {
  def main(args: Array[String]): Unit = {
    val info = """
      |name=xuaner
      |age=18
    """.stripMargin

    println(info)

    val config: Config = ConfigFactory.parseString(info)
    println(config)
    // Config(SimpleConfigObject({"age":18,"name":"xuaner"}))

    val url: URL = new URL("http://ui.learn.com/ui/video.shtml")
    println(url.getHost)
//    println(url.getContent)
    println(url.getDefaultPort)
    println(url.getFile)
    println(url.getProtocol)
    println(url.getQuery)
    println(url.toURI)
    println(url.getUserInfo)
  }

}
