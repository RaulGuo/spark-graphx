package com.proud.test

import org.apache.spark.graphx._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.Graph.graphToGraphOps

object GraphTest {
  val conf = new SparkConf().setAppName("DataImport").setMaster("local").set("spark.sql.warehouse.dir", "file:///root/spark-tmp/warehouse")
  val sc = new SparkContext(conf)
  
  def main(args: Array[String]): Unit = {
    //定义顶点：
    
    //关系包括：lover, roommate, friend, guimi, ex, classmate
//    val spark = SparkSession.builder().appName("DataImport").config("spark.sql.warehouse.dir", "file:///root/spark-tmp/warehouse").enableHiveSupport().getOrCreate()
    
    val vertices = Array((1L, ("赵南", "data analyser")), (2L, ("王巍", "UI designer")), 
    (1L, ("赵南", "data analyser")), (2L, ("王巍", "UI designer")), 
    (3L, ("二哥", "php")), (4L, ("奶源", "php")), 
    (3L, ("二哥", "php")), (4L, ("奶源", "php")), 
    (5L, ("张旋", "Test")), (6L, ("傻阳", "IOS")), 
    (7L, ("小胖", "student")), (8L, ("郭震", "graphx")), 
    (9L, ("周林", "big data")), (10L, ("刘凯", "PHD")), 
    (11L, ("展兴", "lawyer")), (12L, ("王碧璇", "hr")))
    val edges = Array(Edge(1L, 8L, "lover"), Edge(4L, 7L, "lover"), Edge(2L, 3L, "guimi"), Edge(14L, 15L, "guimi"), 
    Edge(8L, 12L, "qianren"), Edge(2L, 8L, "friend"), Edge(3L, 4L, "roommate"), 
    Edge(5L, 6L, "roommate"), Edge(3L, 8L, "roommate"), Edge(8L, 11L, "friend"), 
    Edge(10L, 12L, "friend"), Edge(11L, 12L, "friend"), Edge(9L, 3L, "classmate"))
        
    val peoples:RDD[(VertexId,(String, String))] = sc.parallelize(vertices)
    
    val relations:RDD[Edge[String]] = sc.parallelize(edges)
    
    val school = ("山大", "母校")
    val graph = Graph(peoples, relations, school)
    graph.vertices.take(100)

    //page rank算法测试
    val prGraph = graph.pageRank(0.1)
    
    val rankVertices = prGraph.vertices
    
    val rankEdges = prGraph.edges
    
    rankVertices.foreach(x => println(x._1 +"=>"+ x._2))
    rankEdges.foreach { x => println(x.srcId + "=>" +x.dstId + " = " +x.attr) }
    
    //connected component算法：
    val edgesWithoutZhou = Array(Edge(1L, 8L, "lover"), Edge(4L, 7L, "lover"), Edge(2L, 3L, "guimi"), 
        Edge(10L, 12L, "qianren"), Edge(2L, 8L, "friend"), Edge(3L, 4L, "roommate"), 
        Edge(5L, 6L, "roommate"), Edge(3L, 8L, "roommate"), Edge(8L, 11L, "friend"), 
        Edge(8L, 12L, "friend"), Edge(11L, 12L, "friend"))
    val relationsWithouZhou:RDD[Edge[String]] = sc.parallelize(edgesWithoutZhou)
    val graphWithoutZhou = Graph(peoples, relationsWithouZhou, school)
    
    val cc = graphWithoutZhou.connectedComponents()
    graph.vertices.foreach(x => {println(x._1 + "=>" + x._2)})
    cc.vertices.foreach(x => {println(x._1 + "=>" + x._2)})
    graph.edges.foreach{x => {println(x.srcId + "=>" +x.dstId + " = " +x.attr)}}
    cc.edges.foreach { x => {println(x.srcId + "=>" +x.dstId + " = " +x.attr)} }
    
    //triangle count算法：
    val tc = graph.triangleCount()
    tc.vertices.foreach(x => {println(x._1 + "=>" + x._2)})
    tc.edges.foreach { x => {println(x.srcId + "=>" +x.dstId + " = " +x.attr)} }
    
    //subgraph
    val guozhenGraph = graph.subgraph(epred = {x => {x.srcId == 8L || x.dstId == 8L}})
    guozhenGraph.vertices.foreach(x => {println(x._1 + "=>" + x._2)})
    guozhenGraph.edges.foreach { x => {println(x.srcId + "=>" +x.dstId + " = " +x.attr)} }
    
//    val zhoulinGraph = graph.subgraph(epred = {x => {x.srcId == 9L || x.dstId == 9L}}, vpred = {(id, attr) => {id == 9L}})
    val zhoulinGraph = graph.subgraph(vpred = {(id, attr) => {id == 9L}})
    
    val zhoulinVertices = zhoulinGraph.vertices
    val zhoulinEdges = zhoulinGraph.edges
    zhoulinGraph.vertices.foreach(x => {println(x._1 + "=>" + x._2)})
    zhoulinGraph.edges.foreach { x => {println(x.srcId + "=>" +x.dstId + " = " +x.attr)} }
    
    graph.edges.foreach { x => {println(x.srcId + "=>" +x.dstId + " = " +x.attr)} }
    graph.edges.foreach { x => println(x.attr) }
    
    graph.edges.foreach{
      x => {
        if("roommate".equals(x.attr)){
          println("舍友")
        }else if("friend".equals(x.attr)){
          println("朋友")
        }
      
        println("not it")
      }
    }
    
    //map edges
    val sheyouGraph1:Graph[(String, String), String] = graph.mapEdges(x => {
      if("roommate".equals(x.attr))
        "舍友"
      else
        x.attr
    })
    sheyouGraph1.edges.foreach { x => println(x.attr) }
    
    graph.edges.foreach { x => println(x.attr) }
    
    
    //mapVertices:
    val oneAttrGraph = graph.mapVertices((id, attr) => {
      attr._1+" is:"+attr._2
    })
    oneAttrGraph.vertices.foreach(x => {println(x._1 + "=>" + x._2)})
    
    //mapTriplet
    
    
    //joinVertices
    val outDegree = graph.outDegrees
    graph.outerJoinVertices(outDegree){(id, oldAttr, outDeg) => {
      outDeg match{
        case Some(outDeg) => outDeg
        case None => 0
      }
    }}
    
    
    //aggregateMessages
    graph.aggregateMessages[Int](ctx => ctx.sendToDst(1), _+_ , TripletFields.None)
    
    
    //mapEdges:
    
    
    
    //collect
    
    
    
    //pregel
    
    
    //
    
    
    
    
    
  }
}