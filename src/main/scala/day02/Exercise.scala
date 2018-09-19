package day02

object Exercise {
  def main(args: Array[String]): Unit = {
    //创建一个List
    val list0 = List(2, 5, 9, 6, 7, 1, 8, 3, 4, 0)

    //将list0中的每个元素乘以2后生成一个新的集合
    //_表示取出list0中的元素进行操作
    //ctrl + alt + v 相当于eclipse中的ctrl + 0
    //ctrl + alt + l 格式化
    //ctrl + alt + ->或<-切换面板，与电脑快捷键冲突，还未设置
    val list1 = list0.map(_ * 2)
    println(list0)
    println(list1)

    //将list0中的偶数取出来生成一个新的集合
    val list2 = list0.filter(_ % 2 == 0)
    println(list2)

    //将list0排序(升序)生成一个新的集合
    //调用方法时，如果方法没有参数，不写小括号
    val list3 = list0.sorted
    println(list3)

    //反转
    val list4 = list0.reverse
    println(list4)

    //逆序输出
    val list5 = list0.sorted.reverse
    println(list5)

    //将list0中的元素4个一组，类型为Iterator[List[Int]]
    val it = list0.grouped(4)
    println(it)
//    println(it.toBuffer)
    
    //将Iterator转换成list
    val list6 = it.toList
    println(list6)

    //将多个list压扁成一个list
    val list7 = list6.flatten
    println(list7)

    val lines = List("hello java hello scala", "hello scala", "hello python")
    //map将里面的元素取出来，然后进行操作
    //filter过滤里面的元素
    //先将空格切开，再压平放到一个字符串中
//    val words = lines.map(_.split(" "))
//    println(words)
//    val flatWords = words.flatten
//    println(flatWords)
    val words = lines.flatMap(_.split(" "))
    println(words)

    //并行计算求和
    val arr = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

//    val res = arr.sum
//    println(res)
    //并行计算和线程有关，每一个线程计算一部分
//    val res = arr.par.sum
//    println(res)

    //按照特定的顺序进行聚合
//    val res = arr.reduce(_+_)
//    println(res)

//    val res = arr.par.reduce(_+_)
//    println(res)
//    val res = arr.par.reduce(_-_)
//    println(res)


    //折叠，有初始值（无特定顺序）
//    val res = arr.par.fold(10)(_+_)
//    println(res)

    //折叠，有初始值（无特定顺序）
//    val res = arr.fold(10)(_+_)
//    println(res)

    val list8 = List(List(1,2,3), List(3,4,5),List(2),List(0))
//    val res = list8.flatten.reduce(_+_ )
//    println(res)

    //第一个下划线代表初始值，这里是0
    //第二个下划线代表每次拿到的小的List
    //第一个参数代表小的List进行聚合的结果
    //第二个参数是将所有的小的List聚合的结果进行聚合，得到大的List聚合结果
    var res = list8.aggregate(0)(_+_.sum, _+_)

  }
}
