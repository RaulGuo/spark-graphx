package com.proud.test

import com.proud.ark.db.DBUtil
import org.apache.spark.sql.SparkSession
import com.proud.ark.config.ConfigUtil
import com.proud.ark.data.HDFSUtil

object AppTest {
  case class Link(id:Long, link:String)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Test").config("spark.sql.warehouse.dir", ConfigUtil.warehouse).master(ConfigUtil.master).getOrCreate()
    val df = DBUtil.loadDFFromTable("investment.chuchuang_fazhan", spark).select("id", "link").filter(x => x.getAs[String]("link") != null && !x.getAs[String]("link").isEmpty())
    import spark.implicits._
    
    val result = df.map(x => Link(x.getAs[Long]("id"), split(x.getAs[String]("link"))))
    DBUtil.saveDFToDB(result, "test.hello")
    DBUtil.insertOnUpdateDFToDB(result, "investment.chuchuang_fazhan")
//    HDFSUtil.saveDSToHDFS("/home/data_center/gudong_erji", df)
  }
  
  def split(str:String):String = {
    val length = str.length()/2
    val str1 = str.substring(0, length)
    val str2 = str.substring(length, str.length())
    if(str1.equals(str2)){
      return str1
    }
    return str
  }
}