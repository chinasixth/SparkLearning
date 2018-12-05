package com.sixth.group07.day01

/**
  * @ Author ：liuhao
  * @ Date   ：Created in 11:34 2018/7/23
  * @ 斐波那契数列
  * 递归方法必须有返回值
  */
object ScalaTest02 {
  def main(args: Array[String]): Unit = {
    println(fab(5))
  }

  def fab(n: Int): Int = {
    if (n <= 2) 1 else (fab(n - 1) + fab(n - 2))
  }
}
