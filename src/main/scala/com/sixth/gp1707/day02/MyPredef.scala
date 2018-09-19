package com.sixth.gp1707.day02

/**
  * @ Author ：liuhao
  * @ Date   ：Created in 10:31 2018/7/24
  * @ 
  */
object MyPredef {
  implicit val girlSelect = (girl: MyGirl) => new Ordered[MyGirl] {
    // 有关compare的比较：
    // 如果compare的返回值>0,返回的是girl，也就是调用compare方法的这个对象
    // 如果compare的返回值<0，返回的是that，也就是传入compare作为参数的那个对象
    override def compare(that: MyGirl) = {
      if (girl.faceValue != that.faceValue) {
        girl.faceValue - that.faceValue
      } else {
        that.age - girl.age
      }
    }
  }

  /*
  * ordered和ordering的区别：
  * ordered的compare方法有一个参数
  * ordering的compare方法有两个参数
  * */
  implicit object OrderingGirl extends Ordering[MyGirl] {
    override def compare(x: MyGirl, y: MyGirl): Int = {
      if (x.faceValue == y.faceValue) {
        y.age - x.age
      } else {
        x.faceValue - y.faceValue
      }
    }
  }

}
