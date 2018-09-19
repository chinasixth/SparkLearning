package com.sixth.gp1707.day01


/**
  * @ Author ：liuhao
  * @ Date   ：Created in 10:13 2018/7/23
  * @ 
  */

// 注意class类中不能有main方法
object ScalaTest01 {
  // 一般能用val修饰的变量尽量不要用var修饰
  def main(args: Array[String]): Unit = {
//    val name: String = "tuoer"

    // 同时定义多个变量，封装到一个Array
    //    val Array(a, b, c) = Array(1,"aaa", true)
    // 用下划线定义的值是取不出来的
//    val Array(a, _, _) = Array(1, "aaa", true)

    //    val str: String = "$name is my goddess"
    // 这种语法，一般用在解析key/value键值对,也就是配置文件信息
//    val str =
//    """name=tuoer
//      |age=18
//    """.stripMargin

    // 这个还具有转义的作用，里面的"并没有报错，显然是自动转义了
//    val str =
//      """
//         {"a":"123","b":"456"}
//      """.stripMargin

    val arr = Array(6,5,4,3,2,1)

//    println(arr.reduce(_+_))
//    println(arr.reduceLeft(_+_))
    val map2 = Map(("d",4),("c",3),("a",2),("b",1))
    // aggregate是给定一个初始值放在第一个括号内，然后第二个括号中的第一个函数利用这个初始值去和指定的map2中的值按照函数进行操作，
    // 然后将操作的结果放在这个初始值的位置，用这个结果继续向后进行计算
    // 这里要注意，aggregate第二个参数列表中要给两个函数作为参数，其中第一个函数中的第一个下划线是指第一个括号中的值，往后就是第一个函数的返回值
//    println(map2.aggregate(10)((_: Int)+(_: (String, Int))._2, (_: Int)+(_: Int)))
//    println(map2.reduce((_: (String, Int))._2+(_: (String, Int))._2))

//    println(arr.fold(10)((_: Int)+(_: Int)))

    // map的参数是一个函数，map是将arr中的每一项取出来，并传入函数中
//    val res: Array[Int] = arr.map((x:Int) => x)

    // 定长数组里面的元素打印不出来，变长数组里面的元素可以打印出来，所以将arr转换成变长数组
//    println(res.toBuffer)
    // foreach的参数也是一个函数
//    res.foreach(x => println(x))
//    res.foreach(println(_))
//    res.foreach(println)
    /*
    * foreach和map的区别：
    * foreach没有返回值
    * map有返回值
    * foreach用于不需要返回值的地方，比如将数据取出来放入数据库，并不需要返回值
    * */


    // 需求：把元组的偶数打印出来
//    println(arr.filter((x:Int) => x%2 == 0).toBuffer)
//    println(arr.filter(_%2 == 0).toBuffer)

    // 函数：
    // (x:Int, y:Int) => x * y 表示是一个匿名函数
    // f1表示将匿名函数给变量f1
//    val f1 = (x:Int, y:Int) => x * y
//    val f2 = (x:Int) => x
    // 调用函数
//    arr.map(x => f2(x))
//    arr.map(f2(_))
    // 自动传参
//    arr.map(f2)

    // 另外一种定义函数的方式
//    val f3:Int => Int = {x => x*x}
    // 将等号分为两部分，前面是指定函数的参数类型返回值类型，后面是函数的参数列表和具体的函数体，
    // 前后两部分的分隔是使用 =>
//    val f3:(Int, Int) => Int = {(x, y) => x*y}

    // 降序排序
//    arr.sortWith(_>_)
//##########################################################################
    // map
//    val person = Map(("qingqing",24),("tuoer",18))
    // 返回值是Some()
//    val maybeInt: Option[Int] = person.get("tuoer")
    // 返回值是value值的类型的
//    val i: Int = person.getOrElse("langer", 9)
//    println(maybeInt)

    // 遍历map中的每一个key
//    for(key <- person.keySet){
//      println(key)
//    }
    // 遍历value
//    for(v <- person.values){
//      println(v)
//    }

    // 遍历map中的entryMap
//    for((k,v) <- person){
//      println(s"$k--->$v")
//    }

    // 存储的时候按照Map的key排序
//    val map = SortedMap(("c",1),("d",1),("b",1),("a",1))
//    println(map)

    // 将Array转换成map
//    val arr1: Array[(String, Int)] = Array(("a",1),("b",1))
//    val map: Map[String, Int] = arr1.toMap
//    println(map)
//    #######################################################################3
    // 元组
    // 元组最多可以写22个值，因为元组在Scala中是一个class
//    val tup: (Int, String, Boolean, Double) = (1,"abc", true, 3.14)

    // 一般在使用的时候，一般封装两个值，称为对偶元组
    val tup: (String, String) = ("name", "xiaofang")

    // 取值，注意：元组的下标是从1开始的
//    println(tup._1+"  "+ tup._2)

    // 遍历元组,必须先将元组转换成迭代器，然后打印出来
//    tup.productIterator.foreach(i => println(i))

    // 交换元组中的值,只能是对偶元组
    val tupSwap: (String, String) = tup.swap
    println(tupSwap)

//    ######################################################################33


  }

//  def m1():Int ={
    // 赋值的过程不能作为返回值
    //    val a = 1
    // 一个值也可以作为返回值
    //    a
    //scala中的return作用：return后面的语句是不会被执行的
//  }

  // 定义方法
//  def m1(x: Int, y: Int): Int = x * y
  //指定返回值类型为Unit
//  def m1(x:Int):Unit = x*x
  // 默认指定返回值类型
//  def m1(x:Int) = x*x
  // 是否有返回值类型是由"="决定的
//  def m1(x:Int) {x*x}


}
