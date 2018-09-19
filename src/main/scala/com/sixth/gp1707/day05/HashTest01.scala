package com.sixth.gp1707.day05

import org.apache.spark.HashPartitioner

/**
  * @ Author ：liuhao
  * @ Date   ：Created in 16:57 2018/7/27
  * @ 
  */
object HashTest01 {
  def main(args: Array[String]): Unit = {
    new HashPartitioner(4)
  }
}
