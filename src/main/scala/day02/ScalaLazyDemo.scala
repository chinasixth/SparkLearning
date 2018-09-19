package day02

/**
  * 双击shift，打开搜索框
  * 在Scala中用lazy定义的变量叫惰性变量，会实现延迟加载
  * 惰性变量只能是不可变的变量，且只有在调用惰性变量时，才会去实例化这个变量
  */
class ScalaLazyDemo{

}
object ScalaLazyDemo1{//相当于ScalaLazyDemo的伴生对象，可以看成是静态类
  def init(): Unit = {//返回值为Unit类型，相当于void，返回值结果是()
    println("call init()")
  }

  def main(args: Array[String]): Unit = {//main方法只能写在静态类中
    val property = init()//没有用Lazy关键字修饰的
    println("after init()")
  }
}

object ScalaLazyDemo2{
  def init(): Unit = {//返回值为Unit类型，相当于void，返回值结果是()
    println("call init()")
  }

  def main(args: Array[String]): Unit = {
    //lazy修饰的变量，一开始没有参与编译，后来执行到的时候才编译的。
    lazy val property = init()//用lazy关键字修饰的
    println("after init()")
    println(property)
  }
}