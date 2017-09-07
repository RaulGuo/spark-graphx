package com.proud.graph.investment

import com.proud.ark.db.DBUtil
import com.proud.ark.config.ConfigUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import com.proud.ark.data.HDFSUtil
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._

object ChiguBaseData {
  case class ReportInfo(reportId:Long, companyId:Long, year:Int)
  case class ReportInvestInfo(reportId:Long, amount:Double, name:String)
  case class Bili(company_id:Long, gudong_name:String, percent:Double, amount:Double, total:Double)
  case class BiliStructure(company_id:Long, company_md5:String, company_name:String, gudong_name:String, gudong_id:Long, gudong_md5:String, percent:Double, amount:Double, total:Double)
  case class XuniId(id:Long, name:String)
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master(ConfigUtil.master).appName("ChiguBase").config("spark.sql.warehouse.dir", ConfigUtil.warehouse).master(ConfigUtil.master).getOrCreate()
		
    val companyDF = HDFSUtil.loadCompanyNoGetiBasic(spark).select("id", "name", "md5")
    companyDF.createOrReplaceTempView("company")
    import spark.implicits._
    
    import org.apache.spark.sql.SparkSession._
    val rdd = DBUtil.loadDFFromTable("dc_import.company_report_base", 12, spark).select("id", "company_id").map( x => (x.getAs[Long]("id"), x.getAs[Long]("company_id"))).rdd
    
    val reportInfo = DBUtil.loadDFFromTable("dc_import.company_report_base", 12, spark).select("id", "company_id", "year").map(x => {
	    val year = x.getAs[String]("year")  
	    if(year != null && year.matches("\\d+"))
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
	  
	  val investDF = DBUtil.loadDFFromTable("dc_import.company_report_stock_investment", 12, spark).select("report_id", "act_amount", "name")
	  
	  //reportId, amount, name
	  val invInfo = investDF.map(x => {
	    ReportInvestInfo(x.getAs[Long]("report_id"), trimNumFromLeft(x.getAs[String]("act_amount")), x.getAs[String]("name"))
	  }).filter(x => x.amount != 0)
	  
	  val biliDF = filterReport.join(invInfo, "reportId").select("companyId", "name", "amount").persist()
	  HDFSUtil.saveDSToHDFS("/home/data_center/biliDF", biliDF, SaveMode.Overwrite)
	  
	  val sumAmount = biliDF.groupBy("companyId").sum("amount").withColumnRenamed("sum(amount)", "sum")
    val biliDS = biliDF.join(sumAmount, "companyId").map(r => {
      val sum = r.getAs[Double]("sum")
      Bili(r.getAs[Long]("companyId"), r.getAs[String]("name"), (math rint r.getAs[Double]("amount")*10000/sum)/10000, r.getAs[Double]("amount"), sum)
    })
	  //计算出企业的各个股东持股比例
	  //company_id:Long, gudong_name:String, percent:Double, amount:Double, total:Double
//    val biliDS = filterReport.join(invInfo, "reportId").select("companyId", "name", "amount").groupByKey(x => x.getAs[Long]("companyId")).flatMapGroups{(companyId:Long, it:Iterator[Row]) => {
//      val sum = it.map { x => x.getAs[Double]("amount") }.reduce(_+_)
//      //结果中保留两位小数
//      if(it.hasNext){
//        println("hello, 123")
//      }
//      val r = it.map { r => {
//          Bili(companyId, r.getAs[String]("name"), (math rint r.getAs[Double]("amount")/sum*10000)/10000, r.getAs[Double]("amount"), sum)
//        }
//      }
//      r
//    }}.persist()
	  biliDS.createOrReplaceTempView("bili_info")
	  
	  println("--------------------"+biliDS.count+"------------")
//	  HDFSUtil.saveDSToHDFS("/home/data_center/biliDS", biliDS.toDF(), SaveMode.Overwrite)
	  
	  val biliWithCompany = spark.sql("select c1.id as company_id, c1.name as company_name, c1.md5 as company_md5, bi.gudong_name, c2.id as gudong_id, c2.md5 as gudong_md5, bi.percent, bi.amount, bi.total "
	      +"from company c1 inner join bili_info bi on c1.id = bi.company_id "
	      +"left join company c2 on bi.gudong_name = c2.name").as[BiliStructure].persist()
	  
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
    }}}
    
	  HDFSUtil.saveDSToHDFS("/home/data_center/biliDS", ds.toDF(), SaveMode.Overwrite)
  }
  
  
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
  
//  val numCharSet = Set('0','1','2','3','4','5','6','7','8','9','.')
//  
//  def trimNumFromLeft(amountStr:String):Double = {
//    val amount = "0"+amountStr.trim()
//    for(i <- 0 until amount.length()){
//      if(!numCharSet.contains(amount.charAt(i))){
//        return amount.substring(0, i).toDouble
//      }
//    }
//    
//    amount.toDouble;
//  }
}