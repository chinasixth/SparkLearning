package com.sixth.gp1707.day02

/**
  * @ Author ：liuhao
  * @ Date   ：Created in 11:05 2018/7/24
  * @ 
  */
class ContextBoundsDemo[T: Ordering] {
  def chooser(first: T, second: T): T = {
    val ord: Ordering[T] = implicitly[Ordering[T]]
    if (ord.gt(first, second))
      first
    else
      second
  }
}

object ContextBoundsDemo {

  def main(args: Array[String]): Unit = {
    import MyPredef.OrderingGirl
    val context = new ContextBoundsDemo[MyGirl]
    val g1: MyGirl = new MyGirl("tuoer", 120, 18)
    val g2: MyGirl = new MyGirl("langer", 100, 16)

    val girl: MyGirl = context.chooser(g1, g2)
    println(girl.name)
  }
}