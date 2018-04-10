package com.proud.graph.investment

import org.apache.spark.graphx._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph.graphToGraphOps
import org.apache.spark.sql.SparkSession
import scala.reflect.ClassTag
import scala.runtime.ScalaRunTime._

import com.google.gson.Gson
import org.apache.spark.sql.SaveMode
import com.proud.ark.db.DBUtil
import com.proud.ark.config.ConfigUtil
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import com.proud.ark.data.HDFSUtil
import com.proud.ark.config.GlobalVariables
import org.apache.spark.rdd.RDD

/**
 * 计算企业的股东信息
 */
object JisuanGudongMaster {
  //要保存结果的表
  val resultTable = "statistics.company_stock_relationship"
  
  //用于对没有id的顶点生成一个虚拟ID
  case class XuniId(id:Long, name:String)
  //股东以及股东对应的企业的信息。
  case class Gudong(gudong_id:Long, gudong_name:String, gudong_md5:String, company_id:Long, company_name:String, company_md5:String)
  
  //顶点属性，分别是 name和md5
  type InvVertexAttr = (String, String)
  //顶点，包括ID和其属性
  type InvVertex = (VertexId, InvVertexAttr)
  
  val gson = new Gson
  
  //股东的关系，顶点的属性是企业的名称和md5
  case class Stock(name:String, company_id:String)
  case class YijiGudong(name:String, company_id:String, children: Array[Stock])
  case class ErjiGudong(name:String, company_id:String, children: Array[YijiGudong])
  case class SanjiGudong(name:String, company_id:String, children: Array[ErjiGudong])
  case class SijiGudong(name:String, company_id:String, children: Array[SanjiGudong])

  def loadGudongInvGraph(spark:SparkSession):Graph[(String, String), Int] = {
    //从数据库中读取股东的相关信息，并按照company_name和gudong_name进行去重。该Dataset用于生成图
    //所需要的顶点RDD和边RDD
    val gudong = yuchuliGudongXinxi(spark)
    
    //获取顶点RDD。
    //顶点可以不用去重，图中会自动去重。
    val vertices:RDD[(VertexId,(String, String))] = gudong.rdd.flatMap {
      g => List(
        (g.company_id,(g.company_name, g.company_md5)),
        (g.gudong_id,(g.gudong_name, g.gudong_md5))
      )
    }
    
    //获取图所使用的边。边的属性设置为1（没什么用）。
    val edges = gudong.rdd.map { g => Edge(g.company_id, g.gudong_id, 1) }
    
    //组装图对象
    val graph = Graph(vertices, edges)
    
    graph
  }
  
  /**
   * 预处理股东信息，将股东数据全部整理出来。用于生成图
   */
  def yuchuliGudongXinxi(spark:SparkSession):Dataset[Gudong] = {
    import spark.implicits._
    //将股东去重的任务交给ZhengliQiyeGudongBySQL中，节省内存。
    //从HDFS中获取计算完成后保存的公司对应的股东信息。
    val gudongDF = HDFSUtil.getDSFromHDFS(GlobalVariables.hdfsGudongXinxi, spark).select("company_id", "company_md5", "company_name", "gudong_name", "gudong_id", "gudong_md5")
    gudongDF.createOrReplaceTempView("tmp_gudong")
    
//    val standaloneNamesDF = spark.sql("select distinct(gudong_name) as gudong_name from tmp_gudong t where t.gudong_id is null")
//    val predicates = Array("gudong_id is null")
//    val standaloneNamesDF = DBUtil.loadDFFromTable(gudongXinxiTable, spark, predicates).select("gudong_name").distinct().withColumn("id", monotonically_increasing_id).map { x =>{
//      val id = x.getAs[Long]("id")
//      val dst_name = x.getAs[String]("gudong_name")
//      XuniId((id+1)*(-1), dst_name)
//    } }
    
    //gudong_id可能是空的，由于图中的顶点必须有id，所以给这些没有id的顶点添加一个id，并将这些id设置成负数（从而在结果中，可以知道哪些顶点是企业库中已有的记录，哪些是企业库中没有的记录）
    val standaloneNamesDF = spark.sql("select distinct(gudong_name) as gudong_name from tmp_gudong t where t.gudong_id is null").withColumn("id", monotonically_increasing_id).map { x =>{
      val id = x.getAs[Long]("id")
      val dst_name = x.getAs[String]("gudong_name")
      XuniId((id+1)*(-1), dst_name)
    } }
    standaloneNamesDF.createOrReplaceTempView("xuni_id")
    
    //将生成虚拟id的dataframe与原有的股东信息关联起来。
    val sql = "select t.*, x.id as xuni_id from tmp_gudong t left join xuni_id x on t.gudong_name = x.name"
    
    val ds = spark.sql(sql).map { x => {
      val gudong_id = x.getAs[Long]("gudong_id")
      if(gudong_id == 0){//gudong_id为0表示gudong_id的原来的值为空。此时，取虚拟id组成股东对象
        Gudong(x.getAs("xuni_id"), x.getAs("gudong_name"), x.getAs("gudong_md5"), x.getAs("company_id"), x.getAs("company_name"), x.getAs("company_md5"))
      }else//若股东id有具体的值，则不需要取虚拟id字段，直接用gudong_id即可。
        Gudong(gudong_id, x.getAs("gudong_name"), x.getAs("gudong_md5"), x.getAs("company_id"), x.getAs("company_name"), x.getAs("company_md5"))
    }}.dropDuplicates("gudong_id", "company_id")//删除gudong_id跟company_id重复的
    
    ds
  }
  
  //处理第一层的股东信息
  def transYijiGudong(attrs:(InvVertexAttr, Array[InvVertex])):YijiGudong = {
    val attrArray:Array[InvVertex] = attrs._2
    val gudongArray = attrArray.map { attr => Stock(attr._2._1, attr._2._2) }
    val yijitouzi = new YijiGudong(attrs._1._1, attrs._1._2, gudongArray )
    yijitouzi
  }
  
  def transErjiGudong(attrs:(InvVertexAttr, Array[(VertexId, InvVertexAttr, Array[InvVertex])])):ErjiGudong = {
    val yijiGudongs = attrs._2.map(attr => {
      val newAttr = (attr._2, attr._3)
      transYijiGudong(newAttr)
    })
    new ErjiGudong(attrs._1._1, attrs._1._2, yijiGudongs)
  }
  
  
  def transSanjiGudong(attrs:(InvVertexAttr, Array[(VertexId, InvVertexAttr, Array[(VertexId, InvVertexAttr, Array[InvVertex])])])):SanjiGudong = {
    val erjiGudongs = attrs._2.map(attr => {
      val newAttr = (attr._2, attr._3)
      transErjiGudong(newAttr)
    })
    new SanjiGudong(attrs._1._1, attrs._1._2, erjiGudongs)
  }
  
  def transSijiGudong(attrs:(InvVertexAttr, Array[(VertexId, InvVertexAttr, Array[(VertexId, InvVertexAttr, Array[(VertexId, InvVertexAttr, Array[InvVertex])])])])):SijiGudong = {
	  val sanjiGudongs = attrs._2.map(attr => {
		  val newAttr = (attr._2, attr._3)
				  transSanjiGudong(newAttr)
	  })
	  new SijiGudong(attrs._1._1, attrs._1._2, sanjiGudongs)
  }
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("GudongTuJisuan").config("spark.sql.warehouse.dir", ConfigUtil.warehouse).master(ConfigUtil.master).getOrCreate()  
    val sc = spark.sparkContext
    import spark.implicits._
    val dbOperation = SaveMode.Append
    //整理图，获取图对象
    val graph = loadGudongInvGraph(spark).persist()
    
//    graph.vertices.filter(x => x._1 == -1649267464721L).take(100)
//    graph.edges.filter { x => x.dstId == -1589137908341L }.take(100)
//    graph.edges.filter{x => x.srcId == 320066999L}.take(100)
    //获得顶点对应的出边的邻接点的集合的RDD
    val vertexWithNeighbourRDD:VertexRDD[Array[InvVertex]] = graph.collectNeighbors(EdgeDirection.Out)
    //将graph跟获得的RDD做关联，将每个graph的顶点属性转化为其出边的邻接点的集合，获得其所有的直接投资
    //:Graph[(InvVertexAttr,Array[InvVertex]), Int]
    //Graph[((String, String), Array[InvVertex]), Int]
    //使用outJoinVertices，将图的顶点属性变成其一级投资的信息
    val graphWithNeighbours:Graph[(InvVertexAttr,Array[InvVertex]), Int] = graph.outerJoinVertices(vertexWithNeighbourRDD){
      case(vid, oriVerVal:InvVertexAttr, neighbours) => (oriVerVal, neighbours.getOrElse(Array()))
    }
    
//    val yijiJson:RDD[(Long, String)] = graphWithNeighbours.vertices.map(vertex => {
//      // vertex => (VertexId, (InvVertexAttr, Array[InvVertex]))
//      // (VertexId, ((JisuanTouziGuanxi.InvVertexAttr, Array[JisuanTouziGuanxi.InvVertex]), Array[String]))
//      val yiji = transYijiGudong(vertex._2)
//      (vertex._1, gson.toJson(yiji))
//    })
//    yijiJson.filter(row => row._1 > 0).map(row => (row._1, row._2, 1)).toDF("company_id", "relationship", "layer").write.mode(dbOperation).jdbc(DBUtil.db207url, "company_stock_relationship", DBUtil.prop)
    
    //将顶点信息的id和一级投资企业ID组合，发送给其对应的src顶点，获取RDD。
    //使用aggregateMessages，获取顶点以及对应的二级投资信息的RDD。
    val vertexWithSecondInvIdsRDD = graphWithNeighbours.aggregateMessages[Array[(VertexId, InvVertexAttr, Array[InvVertex])]](ctx => ctx.sendToSrc(Array((ctx.dstId, ctx.dstAttr._1, ctx.dstAttr._2))), _++_, TripletFields.Dst)
    //将graph与二级投资信息的RDD进行关联，获取的图的顶点属性为企业的二级投资信息
    val erjitouziGraph = graph.outerJoinVertices(vertexWithSecondInvIdsRDD){
      case(vid, oriVerVal, neighbours) => (oriVerVal,neighbours.getOrElse(Array()))
    }
    
//    val erjiJson:RDD[(VertexId, String)] = erjitouziGraph.vertices.map(vertex => {
//      val erji = transErjiGudong(vertex._2)
//      (vertex._1, gson.toJson(erji))
//    })
//    erjiJson.filter(row => row._1 > 0).map(row => (row._1, row._2, 2)).toDF("company_id", "relationship", "layer").write.mode(dbOperation).jdbc(DBUtil.db207url, "company_stock_relationship", DBUtil.prop)
    
    
    //计算顶点的三级投资，先将二级投资图中的顶点的信息连通顶点id组合，发送给其src顶点，获取rdd，然后与图进行组合，从而每个顶点的属性都是其三级投资关系
    val sanjitouziVertexRdd = erjitouziGraph.aggregateMessages[Array[(VertexId, InvVertexAttr, Array[(VertexId, InvVertexAttr, Array[InvVertex])])]](ctx => ctx.sendToSrc(Array((ctx.dstId, ctx.dstAttr._1, ctx.dstAttr._2))), _++_, TripletFields.Dst)
    val sanjitouziGraph = graph.outerJoinVertices(sanjitouziVertexRdd){
      case(vid, oriVerVal, neighbours) => (oriVerVal, neighbours.getOrElse(Array()))
    }
    
//    val sanjiJson:RDD[(VertexId, String)] = sanjitouziGraph.vertices.map(vertex => {
//      val sanji = transSanjiGudong(vertex._2)
//      (vertex._1, gson.toJson(sanji))
//    })
//    DBUtil.saveDFToDB(sanjiJson.filter(row => row._1 > 0).map(row => (row._1, row._2, row._2.length())).toDF("company_id", "relationship", "length"), "test.company_stock_relationship_with_length")
    
    //计算四级投资关系
    val sijitouziVertexRdd = sanjitouziGraph.aggregateMessages[Array[(VertexId, InvVertexAttr, Array[(VertexId, InvVertexAttr, Array[(VertexId, InvVertexAttr, Array[InvVertex])])])]](ctx => ctx.sendToSrc(Array((ctx.dstId, ctx.dstAttr._1, ctx.dstAttr._2))), _++_, TripletFields.Dst)
    val sijitouziGraph = graph.outerJoinVertices(sijitouziVertexRdd){
      case(vid, oriVerVal, neighbours) => (oriVerVal, neighbours.getOrElse(Array()))
    }
    
    //先过滤掉那些没有股东信息的节点。然后将其转化为json
    val sijiJson:RDD[(VertexId, String)] = sijitouziGraph.vertices.filter(x => {
      val array = x._2._2
      array != null && !array.isEmpty
    }).map(vertex => {
      val siji = transSijiGudong(vertex._2)
      val sijiStr = gson.toJson(siji)
      (vertex._1, sijiStr)
    })
    
    DBUtil.truncate(resultTable)
    //将结果转换成DataFrame到数据库表中。
    DBUtil.saveDFToDB(sijiJson.filter(row => row._1 > 0).map(row => (row._1, row._2)).toDF("company_id", "relationship"), resultTable)
    
  }
}