package com.proud.graph.pr

import org.apache.spark.sql.SparkSession
import com.proud.ark.config.ConfigUtil
import org.apache.spark.graphx.GraphLoader
import scala.reflect.ClassTag
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.EdgeDirection
import org.apache.spark.graphx.VertexId
import org.apache.spark.graphx.EdgeTriplet

object PageRankByPregel {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master(ConfigUtil.master).appName("PageRankTest").config("spark.sql.warehouse.dir", ConfigUtil.warehouse).master(ConfigUtil.master).getOrCreate()
    import spark.implicits._
    val graph = GraphLoader.edgeListFile(spark.sparkContext, "file:///home/data_center/spark-data/graphx/followers.txt")
    val initMsg = 1;//pagerank算法每个顶点的初始值为1
    
    val result  = pagerank(graph)
    result.vertices.foreach(x => println(x._1 +" => " +x._2._1))
    result.edges.foreach { x => println(x.srcId+" => "+x.dstId +" = " + x.attr) }
  }
  
  def pagerank[VD:ClassTag, ED:ClassTag](graph:Graph[VD, ED]) = {
    val alpha = 0.85
    val initMsg = 1.0;//pagerank算法每个顶点的初始值为1
    val maxIter = 10;
    val direction = EdgeDirection.Out
    val mergeMsg = (x:Double, y:Double) => x+y
//    val vprog = (vId:VertexId, outDeg:Int, score:Double) => 
    //获取每个顶点的出度
    val graphOutDeg = graph.outerJoinVertices(graph.outDegrees){(vid, vdata, deg) => deg.getOrElse(0)}
    //设置每个边的权重
    val graphWithWeight = graphOutDeg.mapTriplets(e => 1.0/e.srcAttr)
    //设置每个顶点的初始值和alpha，顶点属性分别为：点的权重和alpha，
    val graphInitial = graphWithWeight.mapVertices{(id, attr) => (0.0, 0.0)}
    
    
    val sendMsg = (triplet:EdgeTriplet[(Double, Double), Double]) => {
      val (score, alpha) = triplet.srcAttr
      val weight = triplet.attr
      Iterator((triplet.dstId, score*alpha*weight))
    }
    
    val vprog = (vid:VertexId, vAttr:(Double,Double), receive:Double) => {
      ((math rint (vAttr._1*(1-alpha)+receive)*1000000.0)/1000000.0, vAttr._2)
    }
    
    val resultGraph = graphInitial.pregel(initMsg, maxIter, direction)(vprog, sendMsg, mergeMsg)
    resultGraph
  }
}