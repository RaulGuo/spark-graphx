package com.proud.graph.pr

import com.proud.ark.db.ProdIntranetDBUtil
import org.apache.spark.sql.SparkSession
import com.proud.ark.config.ConfigUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._

object CalcGOTForceValue {
  
  case class VertexAttr(name:String, forceVal:Int)
//  case class EdgeAttr(adv:Double)
  
  def buildGraph(spark:SparkSession):Graph[VertexAttr, Int] = {
    import spark.implicits._
    val verticesDF = ProdIntranetDBUtil.loadDFFromTable("test.character", spark)
    val vertices:RDD[(VertexId, VertexAttr)] = verticesDF.rdd.map(x => {
      val id = x.getAs[Long]("id");
      val name = x.getAs[String]("name")
      (id, VertexAttr(name, 0))
    })
    
    val edges:RDD[Edge[Int]] = ProdIntranetDBUtil.loadDFFromTable("test.duel_result", spark).rdd.map { x => {
      val winner = x.getAs[Long]("winner")
      val loser = x.getAs[Long]("loser")
      val adv = x.getAs[Int]("adv")
      Edge(winner, loser, adv)
      
    } }
    
    Graph(vertices, edges)
  }
  
  def calcDuel(spark:SparkSession, graph:Graph[VertexAttr, Int], maxIter:Int = 1) = {
    //初始武力值为100
    val initMsg:Int = 100;
    //最大迭代次数：
    val activeDir = EdgeDirection.Out
    
    val vprog = (vid:VertexId, vAttr:VertexAttr, forceVal:Int) => {
      VertexAttr(vAttr.name, vAttr.forceVal+forceVal)
    }
    
    //发送信息。src是赢的一方，dst是输的一方。
    val sendMsg = (et:EdgeTriplet[VertexAttr, Int]) => {
      val srcId = et.srcId;
      val srcForceVal = et.srcAttr.forceVal
      val dstId = et.dstId
      val dstForceVal = et.dstAttr.forceVal
      val advVal = et.attr
      if(math.abs(srcForceVal-dstForceVal-advVal) > 2){
        Iterator((srcId, if(srcForceVal > dstForceVal) -1 else 1), (dstId, if(srcForceVal < dstForceVal) -1 else 1))
      }else{
        Iterator.empty
      }
    }
    
    val merge = (x:Int, y:Int) => x+y
    
    graph.pregel(initMsg, maxIter, activeDir)(vprog, sendMsg, merge)
  }
  
  
  def printGraph(graph:Graph[VertexAttr, Int]) = {
    graph.vertices.take(100).foreach(println)
  }
  
  def totalScore(graph:Graph[VertexAttr, Int]) = {
    graph.vertices.map { x => x._2.forceVal }.reduce(_+_)
  }
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master(ConfigUtil.master).appName("JisuanChiguBili").config("spark.sql.warehouse.dir", ConfigUtil.warehouse).master(ConfigUtil.master).getOrCreate()
    val graph = buildGraph(spark);
    printGraph(graph)
    
    val graph1 = calcDuel(spark, graph, 1)
    printGraph(graph1)
    
    val graph2 = calcDuel(spark, graph, 10)
    printGraph(graph2)
    
    
  }
  
}