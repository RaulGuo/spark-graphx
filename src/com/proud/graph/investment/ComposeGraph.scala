package com.proud.graph.investment

import org.apache.spark.sql.SparkSession
import com.proud.ark.config.ConfigUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import scala.reflect.ClassTag

object ComposeGraph {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master(ConfigUtil.master).appName("JisuanChiguBili").config("spark.sql.warehouse.dir", ConfigUtil.warehouse).master(ConfigUtil.master).getOrCreate()
    import spark.implicits._
    
    val sc = spark.sparkContext
    
val vertices:RDD[(VertexId, (String, Int))] = sc.parallelize(Array((1L, ("cr7", 27)), (2L, ("messi", 25)), (3L, ("kaka", 29)), (4L, ("Roben", 30)), (4L, ("Xavi", 33)), (5L, ("Raul Guo", 22))))
val edges:RDD[Edge[String]] = sc.parallelize(Array(Edge(1L, 2L, "compete"), Edge(2L, 3L, "teammate"), Edge(1L, 3L, "friend"), Edge(4L, 1L, "hate"), Edge(4L, 2L, "admire")))
val graph = Graph(vertices, edges)


var sccGraph = graph.mapVertices { case (vid, _) => vid }
    // graph we are going to work with in our iterations
    var sccWorkGraph = graph.mapVertices { case (vid, _) => (vid, false) }.cache()

    var numVertices = sccWorkGraph.numVertices
    var iter = 0

//如果一个顶点只有入点，没有出点，或者只有出点没有入点，或者都没有，就将其标记为true。标记为true的顶点随后排除掉
sccWorkGraph = sccWorkGraph.outerJoinVertices(sccWorkGraph.outDegrees){
(vid, data, degreeOpt) => if (degreeOpt.isDefined) data else (vid, true)
}.outerJoinVertices(sccWorkGraph.inDegrees) {
(vid, data, degreeOpt) => if (degreeOpt.isDefined) data else (vid, true)
}.cache()

val finalVertices = sccWorkGraph.vertices.filter{ case (vid, (scc, isFinal)) => isFinal}.mapValues { (vid, data) => data._1}
//获取被排除掉的顶点
finalVertices.collect().foreach(x => println(x._1+" => "+x._2))

sccGraph = sccGraph.outerJoinVertices(finalVertices) {
(vid, scc, opt) => opt.getOrElse(scc)
}

sccGraph.vertices.collect().foreach(x => println(x._1+" => "+x._2))
// only keep vertices that are not final
sccWorkGraph = sccWorkGraph.subgraph(vpred = (vid, data) => !data._2).cache()

sccWorkGraph.vertices.collect().foreach(x => println(x._1 + "=>"+x._2._1+","+x._2._2))
    val conn = graph.connectedComponents()
    conn.vertices.collect().foreach(x => println(x._1 +"=>"+x._2))
    
    
    graph.staticPersonalizedPageRank(1L, 1)
//    val rankGraph = graph.pageRank(0.1)
//    rankGraph.vertices.collect().foreach(x => println(x._1+" => "+x._2))
//    rankGraph.edges.collect().foreach { x => println(x.srcId +" ="+x.attr+"> "+x.dstId) }
//获取顶点对应的出度   
val rankGraph1 = graph.outerJoinVertices(graph.outDegrees)((vid, vdata, deg) => deg.getOrElse(0))    
//每条边的属性变成其源顶点的的出度分之一
val rankGraph2 = rankGraph1.mapTriplets(e => 1.0/e.srcAttr, TripletFields.Src)
val rankGraph3 = rankGraph2.mapVertices{(id, attr) => 0.15}

//delta：如果两个顶点一致，返回1, 否则返回0

//每个顶点都会收集其相邻节点发送过来的权重并收集
val rankUpdates = rankGraph3.aggregateMessages[Double](ctx => ctx.sendToDst(ctx.srcAttr*ctx.attr), _+_ , TripletFields.Src)
    
val once = graph.staticPageRank(1)

    val cc = graph.connectedComponents()
    
    val vs = cc.vertices
    
    graph.stronglyConnectedComponents(100)
    
    val df = vertices.toDF();
    
    df.rdd.toDebugString
    
    val group = df.groupByKey { x => x.getAs[String]("") }
    
    vertices.checkpoint()
  }
  
  
def printPRGraph(graph: Graph[Double, Double]){
graph.vertices.collect().foreach(x => println(x._1+" => "+x._2))
println("vertices rank"+graph.vertices.collect().map(x => x._2).sum)
graph.edges.collect().foreach { x => println(x.srcId +" ="+x.attr+"> "+x.dstId) }
println("edges rank"+graph.edges.collect().map(x => x.attr).sum)
}

def connComp[VD:ClassTag, ED:ClassTag](graph:Graph[VD, ED], maxIter:Int):Graph[VertexId, ED] = {
  val ccGraph = graph.mapVertices{case(vid, vattr) => vid}
  val initMsg = Long.MaxValue
  def sendMsg(triplet:EdgeTriplet[VertexId, ED]):Iterator[(VertexId, VertexId)] = {
    //如果边的源属性小于目标属性，则triplet发送给目标顶点源顶点的顶点属性
    if(triplet.srcAttr > triplet.dstAttr){
      Iterator((triplet.dstId, triplet.srcAttr))
    }else if(triplet.srcAttr < triplet.dstAttr){
      Iterator((triplet.srcId, triplet.dstAttr))
    }else{
      Iterator.empty
    }
  } 
  
  //顶点对收到的消息进行合并，选出最小的
  def mergeMsg(v1:VertexId, v2:VertexId):VertexId = {
    math.min(v1,v2);
  }
  
  //合并完成后，顶点用自身的属性与收到的消息合并后的结果进行合并，逻辑同样是取出自身属性和收到的合并后的属性的最小值
  def vprog(v1:VertexId, vd:VertexId, v2:VertexId):VertexId = mergeMsg(vd, v2);
  val result = Pregel(ccGraph, initMsg, maxIter, EdgeDirection.Either)(vprog, sendMsg, mergeMsg)
  
  result
}
}