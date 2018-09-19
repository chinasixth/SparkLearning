package com.sixth.gp1707.day06

/**
  * @ Author ：liuhao
  * @ Date   ：Created in 16:19 2018/7/30
  * @ 
  */
object MyPredef {
  // 方式一
  implicit val girlOrdering = new Ordering[Girl] {
    override def compare(x: Girl, y: Girl) = {
      if (x.fv == y.fv) {
        y.age - x.age
      } else {
        x.fv - y.fv
      }
    }
  }
}
