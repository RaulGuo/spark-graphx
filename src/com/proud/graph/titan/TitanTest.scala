package com.proud.graph.titan

import org.apache.spark.sql.SparkSession
import com.proud.ark.config.ConfigUtil

object TitanTest {
  val spark = SparkSession.builder().master(ConfigUtil.master).appName("buildCompanyGraph").config("spark.sql.warehouse.dir", ConfigUtil.warehouse).config("spark.serializer", ConfigUtil.seralizer).getOrCreate()
//  val graph = CompanyGraphBuilder.graph
  def main(args: Array[String]): Unit = {
    val arr = ("123", "456", "789")
//    val tx = graph.newTransaction()
//    tx.addVertex("vName", "hello", "vMd5", "12345");
//		tx.addVertex("vName", "world", "vMd5", "67890");
//		tx.commit()
		println("--------game over----------")
		println(arr)
  }
}