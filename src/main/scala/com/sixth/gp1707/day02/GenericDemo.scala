package com.sixth.gp1707.day02

/**
  * @ Author ：liuhao
  * @ Date   ：Created in 9:54 2018/7/24
  * @ 自定义类型，对两个对象进行比较，需要用泛型实现对象里的某个字段的比较方式
  */
class GenericDemo {

}

class Teacher(val name: String, val faceValue: Int) extends Comparable[Teacher] {
  override def compareTo(o: Teacher): Int = {
    this.faceValue - o.faceValue
  }

  override def toString: String = {
    s"name:$name, faceValue:$faceValue"
  }
}

object Teacher {
  def main(args: Array[String]): Unit = {
    val t1 = new Teacher("tuoer", 120)
    val t2 = new Teacher("langer", 100)

    val arr: Array[Teacher] = Array(t1, t2)

    val sorted: Array[Teacher] = arr.sorted.reverse
    println(sorted(0).toString)

  }
}
