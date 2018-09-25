package TestSpark.mydefine

/**
  * @ Author ：liuhao
  * @ Date   ：Created in 15:43 2018/9/24
  * @ 
  */
object MyPredef {
  // 隐式转换函数，不传参数
  implicit val selectOrdering = new Ordering[Girl] {
    override def compare(x: Girl, y: Girl) = {
      if (x.fv == y.fv)
        y.age - x.age
      else
        x.fv - y.fv
    }
  }

  // 传参，只能传一个
  implicit val selectOrdered = (x:Girl) => new Ordered[Girl]{
    override def compare(that: Girl) = {
      if(x.fv == that.fv)
        that.age - x.age
      else
        x.fv - that.fv
    }
  }

}

