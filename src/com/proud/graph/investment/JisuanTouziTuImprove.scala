package com.proud.graph.investment

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import com.proud.ark.config.ConfigUtil
import com.proud.ark.db.DBUtil
import org.apache.spark.graphx._
import org.apache.spark.sql.SaveMode
import com.google.gson.Gson
import com.proud.ark.config.DBName
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row

/**
 * 计算企业的投资关系。
 * 
 */

object JisuanTouziTuImprove {
  
  val resultTable = s"${DBName.statistics}.company_investment_relationship"
  val sourceTable = s"${DBName.dc_import}.company_investment_enterprise"
  val gson = new Gson
  
  case class XuniId(id:Long, name:String)
  case class Touzi(src_id:Long, src_name:String, src_md5:String, dst_id:Long, dst_name:String, dst_md5:String)
  
  /**
   * 对数据库中的企业投资数据进行处理，主要是给dst_id为空的被投资企业添加一个虚拟ID
   * 虚拟ID的值必须是负数。
   */
  def yuchuliQiyeTouzi(spark:SparkSession):Dataset[Touzi] = {
    import spark.implicits._
    //字段信息：src_id, src_name, src_md5, dst_id, dst_name, dst_md5
    val qiyeTouzi = DBUtil.loadDFFromTable(sourceTable, spark).select("src_id", "src_name", "src_md5", "dst_id", "dst_name", "dst_md5")
    qiyeTouzi.createOrReplaceTempView("qiye_touzi")
    val getStandaloneNamesSQL = "select distinct(dst_name) as dst_name from qiye_touzi t where t.dst_id is null"
    val standaloneNameDS = spark.sql(getStandaloneNamesSQL).withColumn("id", monotonically_increasing_id).map { x =>{
      val id = x.getAs[Long]("id")
      val dst_name = x.getAs[String]("dst_name")
      XuniId((id+1)*(-1), dst_name)
    }}
    standaloneNameDS.createOrReplaceTempView("xuni_id")
    
    val sql = "select t.*, x.id as xuni_id from qiye_touzi t left join xuni_id x on t.dst_name = x.name"
    
    val ds = spark.sql(sql).map { x => {
      val dst_id = x.getAs[Long]("dst_id")
      if(dst_id == 0){
        Touzi(x.getAs("src_id"), x.getAs("src_name"), x.getAs("src_md5"), x.getAs("xuni_id"), x.getAs("dst_name"), x.getAs("dst_md5"))
      }else
        Touzi(x.getAs("src_id"), x.getAs("src_name"), x.getAs("src_md5"), dst_id, x.getAs("dst_name"), x.getAs("dst_md5"))
    }}
    
    ds
  }
  
  //将获取的企业投资转换成图，ID作为顶点ID，顶点属性是企业名字和md5，边的属性默认设置为1（不参与计算）
  //图的顶点属性是企业名称和企业的md5。这些都不参与计算
  def loadInvestGraph(df:Dataset[Touzi]):Graph[(String, String), Int] = {
    val vertices:RDD[(VertexId,(String, String))] = df.rdd.flatMap { 
      t => List(
        (t.src_id.asInstanceOf[VertexId],(t.src_name, t.src_md5)),
        (t.dst_id.asInstanceOf[VertexId],(t.dst_name, t.dst_md5))
      )
    }.distinct()
    
    val edges = df.rdd.map { t => Edge(t.src_id.asInstanceOf[VertexId], t.dst_id.asInstanceOf[VertexId], 1) }
    
    val graph = Graph(vertices, edges)
    graph
  }
  
  //顶点的name和md5属性
  type InvVertexAttr = (String, String)
  //顶点的基本属性，分别是ID，name和md5
  type InvVertex = (VertexId, InvVertexAttr)
  //企业信息的基本属性，名称和md5
  case class BasicAttr(name:String, company_id:String)
  case class Stock(name:String)
  case class NameChildren(name:String, children:Array[BasicAttr])
  //一级投资关系的结构
  case class YijiTouzi(name:String, company_id:String, children:Array[BasicAttr])
  case class YijiTouziWithStock(name:String, company_id:String, children:Array[NameChildren], stocks:Array[String])
  //二级投资关系的结构：
  //((String, String), Array[(VertexId, InvVertexAttr, Array[InvVertex])])
  case class ErjiTouzi(name:String, company_id:String, children:Array[YijiTouzi])
  case class ErjiTouziWithStock(name:String, company_id:String, children:Array[YijiTouzi], stocks:Array[String])
  
  //三级投资关系的结构  
  case class SanjiTouzi(name:String, company_id:String, children:Array[ErjiTouzi])
  case class SanjiTouziWithStock(name:String, company_id:String, children:Array[ErjiTouzi], stocks:Array[String])
  
  //四级投资关系的结构
  case class SijiTouzi(name:String, company_id:String, children:Array[SanjiTouzi])
  case class SijiTouziWithStock(name:String, company_id:String, children:Array[SanjiTouzi], stocks:Array[String])
  
  def transYijiTouzi(attrs:(InvVertexAttr, Array[InvVertex])):YijiTouzi = {
    val attrArray:Array[InvVertex] = attrs._2
    val basicAttrArray = attrArray.map { attr => BasicAttr(attr._2._1, attr._2._2) }
//    val duiwaitouzi = new NameChildren("对外投资", basicAttrArray)
    val yijitouzi = new YijiTouzi(attrs._1._1, attrs._1._2, basicAttrArray )
    yijitouzi
  }
  
  //(JisuanTouziGuanxi.InvVertexAttr, Array[JisuanTouziGuanxi.InvVertex]), Array[String]
  def transYijiTouziWithStock(oriAttr:((InvVertexAttr, Array[InvVertex]), Array[String])):YijiTouziWithStock = {
    val attrs = (oriAttr._1._1,oriAttr._1._2,oriAttr._2)
    val attrArray:Array[InvVertex] = attrs._2
    val basicAttrArray = attrArray.map { attr => BasicAttr(attr._2._1, attr._2._2) }
    val duiwaitouzi = new NameChildren("对外投资", basicAttrArray)
    val yijitouzi = new YijiTouziWithStock(attrs._1._1, attrs._1._2, Array(duiwaitouzi), attrs._3 )
    yijitouzi
  }
  
  // ((String, String), Array[(VertexId, JisuanTouziGuanxi.InvVertexAttr, Array[JisuanTouziGuanxi.InvVertex])])
  def transErjiTouzi(attrs:(InvVertexAttr, Array[(VertexId, InvVertexAttr, Array[InvVertex])])):ErjiTouzi = {
    val yijiTouzis = attrs._2.map(attr => {
      val newAttr = (attr._2, attr._3)
      transYijiTouzi(newAttr)
    })
    new ErjiTouzi(attrs._1._1, attrs._1._2, yijiTouzis)
  }
  
  //(((String, String), Array[(VertexId, InvVertexAttr, Array[InvVertex])]), Array[String]))
  def transErjiTouziWithStock(attrs:(InvVertexAttr, Array[(VertexId, InvVertexAttr, Array[InvVertex])], Array[String])):ErjiTouziWithStock = {
    val yijiTouzis = attrs._2.map(attr => {
      val newAttr = (attr._2, attr._3)
      transYijiTouzi(newAttr)
    })
    new ErjiTouziWithStock(attrs._1._1, attrs._1._2, yijiTouzis, attrs._3)
  }
  
   //((String, String), Array[(VertexId, InvVertexAttr, Array[(VertexId, InvVertexAttr, Array[InvVertex])])])
  def transSanjiTouzi(attrs:(InvVertexAttr, Array[(VertexId, InvVertexAttr, Array[(VertexId, InvVertexAttr, Array[InvVertex])])])):SanjiTouzi = {
    val erjiTouzis = attrs._2.map(attr => {
      val newAttr = (attr._2, attr._3)
      transErjiTouzi(newAttr)
    })
    new SanjiTouzi(attrs._1._1, attrs._1._2, erjiTouzis)
  }
  
  def transSanjiTouziWithStock(attrs:(InvVertexAttr, Array[(VertexId, InvVertexAttr, Array[(VertexId, InvVertexAttr, Array[InvVertex])])], Array[String])):SanjiTouziWithStock = {
    val erjiTouzis = attrs._2.map(attr => {
      val newAttr = (attr._2, attr._3)
      transErjiTouzi(newAttr)
    })
    new SanjiTouziWithStock(attrs._1._1, attrs._1._2, erjiTouzis, attrs._3)
  }
  
   //((String, String), Array[(VertexId, InvVertexAttr, Array[(VertexId, InvVertexAttr, Array[(VertexId, InvVertexAttr, Array[InvVertex])])])])
  def transSijiTouzi(attrs:(InvVertexAttr, Array[(VertexId, InvVertexAttr, Array[(VertexId, InvVertexAttr, Array[(VertexId, InvVertexAttr, Array[InvVertex])])])])):SijiTouzi = {
	  val sanjiTouzis = attrs._2.map(attr => {
		  val newAttr = (attr._2, attr._3)
				  transSanjiTouzi(newAttr)
	  })
	  new SijiTouzi(attrs._1._1, attrs._1._2, sanjiTouzis)
  }
  
  def transSijiTouziWithStock(attrs:(InvVertexAttr, Array[(VertexId, InvVertexAttr, Array[(VertexId, InvVertexAttr, Array[(VertexId, InvVertexAttr, Array[InvVertex])])])], Array[String])):SijiTouziWithStock = {
	  val sanjiTouzis = attrs._2.map(attr => {
		  val newAttr = (attr._2, attr._3)
				  transSanjiTouzi(newAttr)
	  })
	  new SijiTouziWithStock(attrs._1._1, attrs._1._2, sanjiTouzis, attrs._3)
  }
  
  def calcGraph(df:Dataset[Touzi], spark:SparkSession){
    val dbOperation = SaveMode.Append
    import spark.implicits._
    val graph = loadInvestGraph(df)
    //获得顶点对应的出边的邻接点的属性（包括邻点的ID、name和md5）的集合的RDD，也就是该公司投资了哪些公司。
    val vertexWithNeighbourRDD:VertexRDD[Array[InvVertex]] = graph.collectNeighbors(EdgeDirection.Out)
    //将graph跟获得的RDD做关联，将每个graph的顶点属性转化为其出边的邻接点的集合，获得其所有的直接投资
    //:Graph[(InvVertexAttr,Array[InvVertex]), Int]
    //Graph[((String, String), Array[InvVertex]), Int]
    val graphWithNeighbours:Graph[(InvVertexAttr,Array[InvVertex]), Int] = graph.outerJoinVertices(vertexWithNeighbourRDD){
      case(vid, oriVerVal:InvVertexAttr, neighbours) => (oriVerVal, neighbours.getOrElse(Array()))
    }
    
    //    val vertexRdd = graphWithNeighbours.vertices
    
    val yijiJson:RDD[(Long, String)] = graphWithNeighbours.vertices.map(vertex => {
      // vertex => (VertexId, (InvVertexAttr, Array[InvVertex]))
      // (VertexId, ((JisuanTouziGuanxi.InvVertexAttr, Array[JisuanTouziGuanxi.InvVertex]), Array[String]))
      val yiji = transYijiTouzi(vertex._2)
      (vertex._1, gson.toJson(yiji))
    })
    
//    yijiJson.saveAsTextFile("F:\\investment\\yijijson")
//    yijiJson.filter(row => row._1 > 0).map(row => (row._1, row._2, 1)).toDF("company_id", "relationship", "layer").write.mode(dbOperation).jdbc(DBUtil.db207url, "company_investment_relationship", DBUtil.prop)
    //此时新生成的图的第一个属性是顶点的原有属性，第二个属性是该企业的一级投资关系的企业的数组
    //保存一下看看结果，加mapValues是因为Array对象直接打印出来的结果是对象的内存地址
//    graphWithNeighbours.vertices.mapValues { x => stringOf(x) }.saveAsTextFile("F:\\investment\\yijiWithName")
    
    
    //将顶点信息的id和一级投资企业ID组合，发送给其对应的src顶点，获取RDD。
    val vertexWithSecondInvIdsRDD = graphWithNeighbours.aggregateMessages[Array[(VertexId, InvVertexAttr, Array[InvVertex])]](ctx => ctx.sendToSrc(Array((ctx.dstId, ctx.dstAttr._1, ctx.dstAttr._2))), _++_, TripletFields.Dst)
    val erjitouziGraph = graph.outerJoinVertices(vertexWithSecondInvIdsRDD){
      case(vid, oriVerVal, neighbours) => (oriVerVal,neighbours.getOrElse(Array()))
    }
    
    
    val erjiJson:RDD[(VertexId, String)] = erjitouziGraph.vertices.map(vertex => {
      val erji = transErjiTouzi(vertex._2)
      (vertex._1, gson.toJson(erji))
    })
//    erjiJson.saveAsTextFile("F:\\investment\\erjijson")
//    erjiJson.filter(row => row._1 > 0).map(row => (row._1, row._2, 2)).toDF("company_id", "relationship", "layer").write.mode(dbOperation).jdbc(DBUtil.db207url, "company_investment_relationship", DBUtil.prop)
    
    
    //计算顶点的三级投资，先将二级投资图中的顶点的信息连通顶点id组合，发送给其src顶点，获取rdd，然后与图进行组合，从而每个顶点的属性都是其三级投资关系
    val sanjitouziVertexRdd = erjitouziGraph.aggregateMessages[Array[(VertexId, InvVertexAttr, Array[(VertexId, InvVertexAttr, Array[InvVertex])])]](ctx => ctx.sendToSrc(Array((ctx.dstId, ctx.dstAttr._1, ctx.dstAttr._2))), _++_, TripletFields.Dst)
    val sanjitouziGraph = graph.outerJoinVertices(sanjitouziVertexRdd){
      case(vid, oriVerVal, neighbours) => (oriVerVal, neighbours.getOrElse(Array()))
    }
    
    val sanjiJson:RDD[(VertexId, String)] = sanjitouziGraph.vertices.map(vertex => {
      val sanji = transSanjiTouzi(vertex._2)
      (vertex._1, gson.toJson(sanji))
    })
    
//    sanjiJson.filter(row => row._1 > 0).map(row => (row._1, row._2, 3)).toDF("company_id", "relationship", "layer").write.mode(dbOperation).jdbc(DBUtil.db207url, "company_investment_relationship", DBUtil.prop)
    //计算四级投资关系
    val sijitouziVertexRdd = sanjitouziGraph.aggregateMessages[Array[(VertexId, InvVertexAttr, Array[(VertexId, InvVertexAttr, Array[(VertexId, InvVertexAttr, Array[InvVertex])])])]](ctx => ctx.sendToSrc(Array((ctx.dstId, ctx.dstAttr._1, ctx.dstAttr._2))), _++_, TripletFields.Dst)
    val sijitouziGraph = graph.outerJoinVertices(sijitouziVertexRdd){
      case(vid, oriVerVal, neighbours) => (oriVerVal, neighbours.getOrElse(Array()))
    }
    
    
    val sijiJson:RDD[(VertexId, String)] = sijitouziGraph.vertices.map(vertex => {
      val siji = transSijiTouzi(vertex._2)
      (vertex._1, gson.toJson(siji))
    })
//    sijijitouziGraph.vertices.mapValues(x => stringOf(x)).saveAsTextFile("F:\\investment\\sijiWithName")
//    val sijitouziRdd = sijitouziGraph.vertices.join(gudongAggreRdd)
//    println("--------------------------四级大小"+sijitouziRdd.count()+"--------------")
//    val sijiJson:RDD[(VertexId, String)] = sijitouziRdd.map(vertex => {
//      val siji = transSijiTouziWithStock((vertex._2._1._1, vertex._2._1._2, vertex._2._2))
//      (vertex._1, gson.toJson(siji))
//    })
//    sijiJson.saveAsTextFile("F:\\investment\\sijijson")
    //要注意这里保存的表要手动创建，这里的SaveMode也不能选Overwrite。因为结果字段太长，超过了默认的TEXT字段长度。
    DBUtil.truncate(resultTable)
    DBUtil.saveDFToDB(sijiJson.filter(row => row._1 > 0).map(row => (row._1, row._2)).toDF("company_id", "relationship"), resultTable)
//    sijiJson.filter(row => row._1 > 0).map(row => (row._1, row._2, 4)).toDF("company_id", "relationship", "layer")//.write.mode(dbOperation).jdbc(DBUtil.db207url, "test.company_investment_relationship", DBUtil.prop)

  }
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("TouziTuJisuanImprove").config("spark.sql.warehouse.dir", ConfigUtil.warehouse).master(ConfigUtil.master).getOrCreate()
    import spark.implicits._
    
    val ds = yuchuliQiyeTouzi(spark)
    calcGraph(ds, spark)
  }
}