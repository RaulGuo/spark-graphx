package com.proud.graph.pr

import org.apache.spark.graphx.GraphLoader
import org.apache.spark.sql.SparkSession
import com.proud.ark.config.ConfigUtil

object PageRankTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master(ConfigUtil.master).appName("PageRankTest").config("spark.sql.warehouse.dir", ConfigUtil.warehouse).master(ConfigUtil.master).getOrCreate()
    import spark.implicits._
    val graph = GraphLoader.edgeListFile(spark.sparkContext, "graphx/followers.txt")
    val pr = graph.pageRank(0.0001)
    val prVertices = pr.vertices
    pr.vertices.foreach(x => println(x._1 +" => " +x._2))
    pr.edges.foreach { x => println(x.srcId+" => "+x.dstId +" = " + x.attr) }
    
  }
}