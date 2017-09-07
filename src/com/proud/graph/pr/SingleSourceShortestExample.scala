package com.proud.graph.pr

import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.sql.SparkSession
import com.proud.ark.config.ConfigUtil


object SingleSourceShortestExample {
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master(ConfigUtil.master).appName("SingleSourceShortestPath").config("spark.sql.warehouse.dir", ConfigUtil.warehouse).master(ConfigUtil.master).getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext
    val graph:Graph[Long, Double] = GraphGenerators.logNormalGraph(sc, 100).mapEdges(x => x.attr.toDouble)
    val sourceId:VertexId = 42;
    
    //将graph的顶点属性设置为无限大（除了原点）
    val initGraph = graph.mapVertices((id, _) => if(id == sourceId) 0.0 else Double.MaxValue)
    
    val initMsg = Double.MaxValue
    
    val maxIter = Int.MaxValue
    
    val edgeDirection = EdgeDirection.In
    
    val vprog = (id:VertexId, dist:Double, newDist:Double) => math.min(dist, newDist)
    
    val sendMsg = (triplet:EdgeTriplet[Double, Double]) => {
      if(triplet.srcAttr + triplet.attr > triplet.dstAttr){
        Iterator((triplet.dstId, triplet.srcAttr+triplet.attr))
      }else{
        Iterator.empty
      }
    }
    
    val mergeMsg = (a:Double, b:Double) => math.min(a, b)
    
    val sssp = initGraph.pregel(initMsg, maxIter, edgeDirection)(vprog, sendMsg, mergeMsg)
  }
  
  def getShortestPath[VD](graph:Graph[VD, Double], sourceId:VertexId) = {
    val initGraph:Graph[(Double, List[VertexId]), Double] = graph.mapVertices((id, _) => if(id == sourceId) (0.0, List.empty) else (Double.MaxValue, List.empty))
    
    
    val initMsg:(Double, List[VertexId]) = (Double.MaxValue, List.empty)
    
    val maxIter = Int.MaxValue
    
    val edgeDirection = EdgeDirection.In
    
    val vprog = (id:VertexId, a:(Double, List[VertexId]), b:(Double, List[VertexId])) => if(a._1 < b._1){
      a
    }else{
      b
    }
    
    val sendMsg:(EdgeTriplet[(Double, List[VertexId]), Double] => Iterator[(VertexId, (Double, List[VertexId]))]) = (triplet:EdgeTriplet[(Double, List[VertexId]), Double]) => {
      if(triplet.srcAttr._1+triplet.attr < triplet.dstAttr._1){
        Iterator((triplet.dstId, (triplet.srcAttr._1+triplet.attr, triplet.srcAttr._2:+triplet.dstId)))
      }else{
        Iterator.empty
      }
    }
    
    val mergeMsg:(((Double, List[VertexId]), (Double, List[VertexId])) => (Double, List[VertexId])) = (a:(Double, List[VertexId]), b:(Double, List[VertexId])) => {
      if(a._1 < b._1){
        a
      }else{
        b
      }
    }
    
    val sssp = initGraph.pregel(initMsg, maxIter, edgeDirection)(vprog, sendMsg, mergeMsg)
  }
  
  def printGraph[VD, ED](graph:Graph[VD, ED]) = {
    graph.vertices.take(100).foreach(println)
  }
}