package com.proud.graph.titan

import com.proud.ark.data.HDFSUtil
import com.proud.ark.config.GlobalVariables
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset

/**
 * 准备需要用于构建titan 图数据库的基础数据。
 * 图中需要包括的信息和关系：
 * 计算投资、计算股东、计算持股比例以及疑似关系
 * 需要录入的数据包括：
 * 投资数据（企业投资company_report_invest_enterprise，(ZhengliQiyeTouzi)
 * 企业股东company_stock_holder，（ZhengliQiyeGudongBySQL）
 * 以及企业的股东出资company_report_stock_investment(JisuanChiguBiliByGraph)
 * 公司高管
 * 以及其余的所有企业信息（从结果导向来看，不录入也没问题）
 * 
 * 1. 整理企业的所有顶点信息（可以直接通过数据库录入）
 * 2. 将高管、企业出资、股东信息都整理成同一种格式
 * 
 * 
 * 
 */
object PrepareTitanData {
  /**
   * company_id：企业id，必须有值
   * value：用于录入股东出资信息，用作边的属性
   * relation：边的类型，是投资（inv），高管（manage），gudong（stockholder）或是带投资数量的投资（invWith）
   * name：顶点的名字。如果有名字，则inVId一定是空的，因为这代表顶点不存在
   * dst_id：边的in vertex顶点的vId。
   */
  case class TitanObj(company_id:Long, value:Double, relation:String, dst_name:String, dst_id:Long)
  case class InvestObj(company_id:Long, gudong_id:Long)
  
  //整理企业股东信息，将股东信息
  def zhengliGudong(spark:SparkSession):Dataset[TitanObj] = {
    val gudongDF = HDFSUtil.getDSFromHDFS(GlobalVariables.hdfsGudongXinxi, spark).select("company_id", "gudong_id", "gudong_name")
    import spark.implicits._
    
    val result = gudongDF.map(x => {
      if(x.getAs[Long]("gudong_id") == null || x.getAs[Long]("gudong_id") == 0){
        val companyId = x.getAs[Long]("company_id")
        val gudongName = x.getAs[String]("gudong_name")
        TitanObj(companyId, -1, "stockholder", gudongName, -1)
      }else{
        val companyId = x.getAs[Long]("company_id")
        val gudongId = x.getAs[Long]("gudong_id")
        TitanObj(companyId, -1, "stockholder", null, gudongId)
      }
    })
    
    result
  }
  
  def zhengliQiyeTouzi(spark:SparkSession){
    import spark.implicits._
    
  }
  
  def main(args: Array[String]): Unit = {
    
  }
}