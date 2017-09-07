package com.proud.test

class ApplyTest {
  def apply(x:Int):Int = x+1;
  var a:Int = _
	def saySth(sth:String){
  }
}

object ApplyTest {
  def apply(x:Int):Int = x+1;
  
  var b:Long = _
  
  
  def main(args: Array[String]): Unit = {
    val id = ApplyTest(3)
    val test = new ApplyTest()
    println(test.a)
  }
}