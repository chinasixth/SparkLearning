package com.sixth.group07.day02

/**
  * @ Author ：liuhao
  * @ Date   ：Created in 10:10 2018/7/24
  * @ 使用上界
  */
class UpperBoundsDemo[T <: Comparable[T]] {
  def chooser(first: T, second: T): T = {
    if (first.compareTo(second) > 0)
      first
    else
      second
  }
}

object Chooser {
  def main(args: Array[String]): Unit = {
    val chooser = new UpperBoundsDemo[Girl]
    val g1: Girl = new Girl("tuoer", 120, 18)
    val g2: Girl = new Girl("langer", 100, 16)

    val girl: Girl = chooser.chooser(g1, g2)
    println(girl)
  }
}

class Girl(val name: String, val faceValue: Int, val age: Int) extends Comparable[Girl] {
  override def toString: String = s"name:$name, faceValue:$faceValue, age:$age"

  override def compareTo(o: Girl): Int = {
    if(this.faceValue != o.faceValue){
      this.faceValue - o.faceValue
    }else{
      o.age - this.age
    }
  }
}
