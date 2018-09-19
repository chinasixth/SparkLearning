package com.sixth.gp1707.day02

/**
  * @ Author ：liuhao
  * @ Date   ：Created in 10:24 2018/7/24
  * @ 视界：
  * 传统比较两个对象，是根据对象的某个字段进行比较，这需要继承一个如Comparable的接口
  * 然而，在Scala中有隐式转化 ，我们可以给一个对象使用隐式转换来实现方法增强
  * 泛型之视界，就可以实现这样的需求
  * 我们要比较的类，可以不用去继承这个提供比较方法的类，只需要在一个业务类定义的时候指定视界的定义方式即可
  * 然后需要一个隐式转换，在隐式转换中实现比较的方法，当new业务类的之前，先将这个隐式转换import进来即可
  * 这样写的好处是：我们好多类都可以使用这个业务类，将实现比较的方法以隐式转换的形式封装到一个专门的类中，让代码更加的优雅
  */
class ViewBoundsDemo[T <% Ordered[T]] {
  def chooser(first: T, second: T): T = {
    if (first > second)
      first
    else
      second
  }

}

object ViewBoundsDemo {
  def main(args: Array[String]): Unit = {
    import MyPredef.girlSelect

    val choose = new ViewBoundsDemo[MyGirl]
    val g1: MyGirl = new MyGirl("tuoer", 120, 18)
    val g2: MyGirl = new MyGirl("langer", 100, 16)

    val girl: MyGirl = choose.chooser(g1, g2)

    println(girl.name)
  }
}

class MyGirl(val name: String, val faceValue: Int, val age: Int) {

}
