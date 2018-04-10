package com.proud.test

class TestImplicit(v:Int) {
  def add(y:TestImplicit):TestImplicit = {
    new TestImplicit(v+y.getV())
  }
  
  def getV():Int = {
    v
  }
}

object TestImplicit{
  implicit def toInt(x:TestImplicit):Int = x.getV()
  
  def main(args: Array[String]): Unit = {
    val v1 = new TestImplicit(1)
    val v2 = 4+v1
    println(v2)
  }
}