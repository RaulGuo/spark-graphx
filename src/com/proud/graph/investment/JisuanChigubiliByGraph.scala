package com.proud.graph.investment

import com.proud.ark.config.ConfigUtil
import org.apache.spark.sql.SparkSession
import com.proud.ark.db.DBUtil
import com.proud.ark.data.HDFSUtil
import org.apache.spark.sql.Row
import scala.math.BigDecimal
import util.control.Breaks._
import com.proud.ark.config.GlobalVariables
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import com.google.gson.Gson
import org.apache.spark.sql.SaveMode

/**
 * 计算企业的持股比例：
nohup spark-submit --master spark://bigdata01:7077 --class com.proud.graph.investment.ChiguBaseData --driver-memory 1g --executor-memory 12g --jars /home/data_center/dependency/mysql-connector-java.jar,/home/data_center/dependency/gson.jar,/home/data_center/dependency/ArkUtil-0.0.1-SNAPSHOT.jar /home/data_center/dependency/graphx-0.0.1-SNAPSHOT.jar > chigubase.out &
nohup spark-submit --master spark://bigdata01:7077 --class com.proud.graph.investment.JisuanChigubiliByGraph --driver-memory 1g --executor-memory 13g --jars /home/data_center/dependency/mysql-connector-java.jar,/home/data_center/dependency/gson.jar,/home/data_center/dependency/ArkUtil-0.0.1-SNAPSHOT.jar /home/data_center/dependency/graphx-0.0.1-SNAPSHOT.jar > chigubili.out &
ChiguBaseData
spark-shell --master spark://bigdata01:7077 --driver-memory 1g --executor-memory 13g --jars /home/data_center/dependency/mysql-connector-java.jar,/home/data_center/dependency/gson.jar,/home/data_center/dependency/ArkUtil-0.0.1-SNAPSHOT.jar,/home/data_center/dependency/graphx-0.0.1-SNAPSHOT.jar
select x1.* from chigu_xinxi x1 inner join chigu_xinxi x2 on x1.gudong_id = x2.company_id inner join chigu_xinxi x3 on x2.gudong_id = x3.company_id limit 100;
441539534, 441333938,442985099,442311514
 */

object JisuanChigubiliByGraph {
  case class ReportInfo(reportId:Long, companyId:Long, year:Int)
	case class ReportInvestInfo(reportId:Long, amount:Double, name:String)
	case class BiliStructure(company_id:Long, company_md5:String, company_name:String, gudong_name:String, gudong_id:Long, gudong_md5:String, percent:Double, amount:Double, total:Double)

	case class Bili(company_id:Long, gudong_name:String, percent:Double, amount:Double, total:Double)
	case class BiliWithTag(company_id:Long, gudong_name:String, percent:Double, amount:Double, total:Double, reserve:Boolean)
	case class XuniId(id:Long, name:String)
	case class EdgeAttr(percent:Double, amount:Double, total:Double)
	
  val resultTable = "statistics.chigu_bili"
  //顶点的属性，分别代表name和md5
	case class VtxAttr(company_name:String, company_md5:String)
	//顶点收集邻居节点和边的属性。
	case class VtxEdgeAttr(gudong_name:String, gudong_md5:String, percent:Double, amount:Double, total:Double)
	case class YijiVtxAttr(company_name:String, company_md5:String, neighbour:Array[VtxEdgeAttr])
	
	//作为二级图的顶点属性，顶点属性是他的集合
	case class ErjiVtxEdgeAttr(gudong_name:String, gudong_md5:String, percent:Double, amount:Double, total:Double, neighbour:Array[VtxEdgeAttr])
	case class ErjiVtxAttr(company_name:String, company_md5:String, neighbour:Array[ErjiVtxEdgeAttr])
	
	case class SanjiVtxEdgeAttr(gudong_name:String, gudong_md5:String, percent:Double, amount:Double, total:Double, neighbour:Array[ErjiVtxEdgeAttr])
	case class SanjiVtxAttr(gudong_name:String, gudong_md5:String, neighbour:Array[SanjiVtxEdgeAttr])
	case class StockChange(company_id:Long, after:Double, gudong_name:String)
	
	val gson = new Gson()
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master(ConfigUtil.master).appName("JisuanChiguBili").config("spark.sql.warehouse.dir", ConfigUtil.warehouse).master(ConfigUtil.master).getOrCreate()
    import spark.implicits._
    val biliDS = generateBasicDS(spark).persist()
    HDFSUtil.saveDSToHDFS("/home/data_center/biliDS_new", biliDS, SaveMode.Overwrite)
//    DBUtil.saveDFToDB(biliDS, "test.bili_dataset")
//		val basicRDD = generateBasicDS(spark).rdd
    val basicRDD = biliDS.rdd
		
//    val basicRDD = HDFSUtil.getDSFromHDFS("/home/data_center/biliDS_new", spark).as[BiliStructure].rdd
		//顶点的属性：包括name和md5。边的属性包括percent，amount，double。边的方向从股东指向企业
		val graph = generateGraph(basicRDD).persist()
		biliDS.unpersist()
		
		//股东顶点向企业顶点发送该股东顶点和边的信息，企业顶点收集信息并处理，合并成图。此时图中的顶点的信息包括其原有信息，以及其下一级的股东的投资信息
		val vertexWithNeighbourRDD:VertexRDD[Array[VtxEdgeAttr]] = graph.aggregateMessages[Array[VtxEdgeAttr]](ctx => ctx.sendToDst(Array(VtxEdgeAttr(ctx.srcAttr.company_name, ctx.srcAttr.company_md5, ctx.attr.percent, ctx.attr.amount, ctx.attr.total))), _++_)
    val graphWithNeighbours:Graph[YijiVtxAttr, EdgeAttr] = graph.outerJoinVertices(vertexWithNeighbourRDD){
      case(vid, origAttr:VtxAttr, neighbours) => {
        YijiVtxAttr(origAttr.company_name, origAttr.company_md5, neighbours.getOrElse(Array()))
      }
    }
    
    val vertexWithErjiNeighbourRDD:VertexRDD[Array[ErjiVtxEdgeAttr]] = graphWithNeighbours.aggregateMessages(ctx => ctx.sendToDst(Array(ErjiVtxEdgeAttr(ctx.srcAttr.company_name,ctx.srcAttr.company_md5, ctx.attr.percent, ctx.attr.amount, ctx.attr.total, ctx.srcAttr.neighbour))), _++_)
    val graphWithErjiNeighbours:Graph[ErjiVtxAttr, EdgeAttr] = graph.outerJoinVertices(vertexWithErjiNeighbourRDD){
      case(vid, origAttr, neighbours) => {
        ErjiVtxAttr(origAttr.company_name, origAttr.company_md5, neighbours.getOrElse(Array()))
      }
    }
    
    val vertexWithSijiNeighbourRDD:VertexRDD[Array[SanjiVtxEdgeAttr]] = graphWithErjiNeighbours.aggregateMessages(ctx => ctx.sendToDst(Array(SanjiVtxEdgeAttr(ctx.srcAttr.company_name,ctx.srcAttr.company_md5, ctx.attr.percent, ctx.attr.amount, ctx.attr.total, ctx.srcAttr.neighbour))), _++_)
    val graphWithSijiNeighbours:Graph[SanjiVtxAttr, EdgeAttr] = graph.outerJoinVertices(vertexWithSijiNeighbourRDD){
      case(vid, origAttr, neighbours) => {
        SanjiVtxAttr(origAttr.company_name, origAttr.company_md5, neighbours.getOrElse(Array()))
      }
    }
    
    val resultDF = graphWithSijiNeighbours.vertices.filter{case(id, attr) => {
      attr.neighbour != null && !attr.neighbour.isEmpty
    }}.map{case(id, attr) => {
      (id, gson.toJson(attr))
    }}.toDF("company_id", "bili_info").filter(x => x.getAs[Long]("company_id") > 0)
    
    DBUtil.truncate(resultTable)
    DBUtil.saveDFToDB(resultDF, resultTable)
  }
  
  def generateBasicDS(spark:SparkSession):Dataset[BiliStructure] = {
    import spark.implicits._
    
    //gudong_id, gudong_name, md5
    val companyDF = HDFSUtil.loadCompanyNoGetiBasic(spark).select("id", "name", "md5")
    companyDF.createOrReplaceTempView("company")
    //reportId, companyId, year
    val reportInfo = DBUtil.loadDFFromTable("dc_import.company_report_base", 32, spark).select("id", "company_id", "year").map(x => {
      val yearStr = x.getAs[String]("year")
	    val year = if(yearStr == null || yearStr.trim().isEmpty()) null else yearStr.replaceAll("[^\\d]", "").trim()
	    
	    if(year != null && !year.isEmpty())
	      ReportInfo(x.getAs[Long]("id"), x.getAs[Long]("company_id"), year.toInt)
	    else
	      ReportInfo(x.getAs[Long]("id"), x.getAs[Long]("company_id"), -1)
	  }).filter(_.year != -1)
	  //过滤出每个公司最新一年的报表，根据最新一年的报表来计算持股比例。
	  //reportId, companyId, year
	  val filterReport = reportInfo.groupByKey(r => r.companyId).reduceGroups{(r1:ReportInfo, r2:ReportInfo) => {
		  if(r1.year > r2.year)
			  r1
		  else
			  r2
	  }}.map(x => x._2)

	  //report_id, act_amount, name
	  val investDF = DBUtil.loadDFFromTable("dc_import.company_report_stock_investment", 32, spark).select("report_id", "act_amount", "name").filter(x => (x.getAs[String]("act_amount") != null && x.getAs[String]("act_amount").trim().length() != 0))
	  
	  //reportId, amount, name
	  val invInfo = investDF.map(x => {
	    ReportInvestInfo(x.getAs[Long]("report_id"), trimNumFromLeft(x.getAs[String]("act_amount")), x.getAs[String]("name"))
	  }).filter(x => x.amount != 0)
	  
//	  val reportInfo1 = DBUtil.loadDFFromTable("dc_import.company_report_base", 12, spark).select("id", "company_id", "year").groupBy("company_id").agg(max(struct("year")) as "event").select("id")
	  val reportInfo1 = DBUtil.loadDFFromTable("dc_import.company_report_base", 32, spark).select("id", "company_id", "year").groupBy("company_id").agg(max(struct("year", "id")) as "max").select("company_id", "max.*")
	  //计算出企业的各个股东持股比例
	  val biliDF = filterReport.join(invInfo, "reportId").select("companyId", "name", "amount").persist()
	  val sumAmount = biliDF.groupBy("companyId").sum("amount").withColumnRenamed("sum(amount)", "sum")
    val reportBiliDS = biliDF.join(sumAmount, "companyId").map(r => {
      val sum = r.getAs[Double]("sum")
      BiliWithTag(r.getAs[Long]("companyId"), r.getAs[String]("name"), (math rint r.getAs[Double]("amount")*10000/sum)/10000, r.getAs[Double]("amount"), sum, false)
    })
    
    
    //增加股权变更的信息
    val stockChangeDS = DBUtil.loadDFFromTable("dc_import.company_stock_change", 32, spark).select("company_id", "after", "stockholder").filter(x => x.getAs[String]("after") != null && !x.getAs[String]("after").contains(":")).map(row => {
      val companyId = row.getAs[Long]("company_id")
      val stockholder = row.getAs[String]("stockholder")
      val after = transAfterToDouble(row.getAs[String]("after"))
      BiliWithTag(companyId, stockholder, after, -1, -1, true)
    }).filter(x => x.percent != 0)
    
    stockChangeDS.filter(x => x.company_id == 10990).show
    
//    val stockIdSet = stockChangeDS.map { x => x.company_id }.distinct().collectAsList()
    val biliDS = reportBiliDS.union(stockChangeDS).groupByKey{ x => x.company_id }.flatMapGroups[Bili]{(companyId:Long, it:Iterator[BiliWithTag]) => {
      //如果有股权变更的信息，就用股权变更的信息
      val resultSet = it.toSet
      val flagSet = resultSet.map { x => x.reserve }
      if(flagSet.contains(true)){
    	  resultSet.filter { x => x.reserve }.map { x => Bili(x.company_id, x.gudong_name, x.percent, x.amount, x.total) }
      }else{
        resultSet.map { x => Bili(x.company_id, x.gudong_name, x.percent, x.amount, x.total)  }
      }
    }}
    
	  biliDS.createOrReplaceTempView("bili_info")
	  
	  val biliWithCompany = spark.sql("select c1.id as company_id, c1.name as company_name, c1.md5 as company_md5, bi.gudong_name, c2.id as gudong_id, c2.md5 as gudong_md5, bi.percent, bi.amount, bi.total "
	      +"from company c1 inner join bili_info bi on c1.id = bi.company_id "
	      +"left join company c2 on bi.gudong_name = c2.name").as[BiliStructure].persist()
	      
	  biliDF.unpersist()
	  
	  biliWithCompany.createOrReplaceTempView("basic_bili_info")
		val standaloneNamesDF = spark.sql("select distinct(gudong_name) as gudong_name from basic_bili_info t where t.gudong_id is null").withColumn("id", monotonically_increasing_id).map { x =>{
      val id = x.getAs[Long]("id")
      val dst_name = x.getAs[String]("gudong_name")
      XuniId((id+1)*(-1), dst_name)
    } }
    standaloneNamesDF.createOrReplaceTempView("xuni_id")
    val sql = "select t.*, x.id as xuni_id from basic_bili_info t left join xuni_id x on t.gudong_name = x.name"
    
    val ds = spark.sql(sql).mapPartitions { it => it.map { x => {
      val gudong_id = x.getAs[Long]("gudong_id")
      if(gudong_id == 0){
        BiliStructure(x.getAs[Long]("company_id"), x.getAs[String]("company_md5"), x.getAs[String]("company_name"), x.getAs[String]("gudong_name"), x.getAs[Long]("xuni_id"), x.getAs[String]("gudong_md5"),x.getAs[Double]("percent"),x.getAs[Double]("amount"),x.getAs[Double]("total"))
      }else
        BiliStructure(x.getAs[Long]("company_id"), x.getAs[String]("company_md5"), x.getAs[String]("company_name"), x.getAs[String]("gudong_name"), gudong_id, x.getAs[String]("gudong_md5"),x.getAs[Double]("percent"),x.getAs[Double]("amount"),x.getAs[Double]("total"))
    }}}.persist()
    
        
    ds
  }
  
  def generateGraph(basicRDD:RDD[BiliStructure]):Graph[VtxAttr, EdgeAttr] = {
    val vertices:RDD[(VertexId,VtxAttr)] = basicRDD.flatMap {
      g => List(
        (g.company_id,VtxAttr(g.company_name, g.company_md5)),
        (g.gudong_id,VtxAttr(g.gudong_name, g.gudong_md5))
      )
    }
    //边的方向：从股东指向边；属性：percent，amount，total
    val edges = basicRDD.map { g => Edge(g.gudong_id, g.company_id, EdgeAttr(g.percent, g.amount, g.total)) }
    
    val graph = Graph(vertices, edges)
    
    graph
  }
  
  //将股权变更中的after转换成具体的数值
  def transAfterToDouble(after:String):Double = {
    if(after == null)
      return 0
    val str = after.replaceAll("[^\\d.]" , "")
    if(str.isEmpty())
      return 0
    else
      (math rint str.toDouble*100)/10000.0d
  }
  
  //对amount进行解析，移除掉字母和小数点之外的任何字符，然后转化为double。若有异常，则认为记录不合法，返回0，在之后的操作中过滤掉。
  def trimNumFromLeft(amountStr:String):Double = {
    try{
      if(amountStr == null || amountStr.trim().isEmpty()){
        return 0
      }
      //取出汉字
      val amount = amountStr.replaceAll("[^\\d.]", "").toDouble
      return amount
    }catch{
      case e:Exception => 0
    }
  }
}