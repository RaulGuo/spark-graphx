package com.proud.graph.titan

import com.proud.ark.db.DBUtil
import com.proud.ark.config.ConfigUtil
import org.apache.spark.sql.SparkSession
import com.proud.ark.data.HDFSUtil
import com.proud.ark.config.GlobalVariables
import org.apache.spark.sql.SaveMode

/**
spark-shell --master spark://bigdata01:7077 --jars /home/data_center/dependency/mysql-connector-java.jar,/home/data_center/dependency/gson.jar,/home/data_center/dependency/ArkUtil-0.0.1-SNAPSHOT.jar
spark-submit --master spark://bigdata01:7077 \
--executor-memory 10g --class com.proud.graph.titan.CompanyGraphBuilder \
--jars /home/data_center/dependency/mysql-connector-java.jar,/home/data_center/dependency/gson.jar,/home/data_center/dependency/ArkUtil-0.0.1-SNAPSHOT.jar \
/home/data_center/dependency/graphx-0.0.1-SNAPSHOT.jar
 */

object CompanyGraphBuilder {
  
  //整理股东信息计算出来的边，添加单独的股东，
  def zhengliGudongEdge(spark:SparkSession){
    val gudongDF = HDFSUtil.getDSFromHDFS(GlobalVariables.hdfsGudongXinxi, spark).select("company_id", "gudong_name", "gudong_id").persist()
    gudongDF.createOrReplaceTempView("gudong")
    
    val edges = spark.sql("select company_id, gudong_id from gudong where gudong_id is not null")
//    HDFSUtil.saveDSToHDFS("/home/data_center/titan/gudong_xinxi_1", edges, SaveMode.Overwrite)
    DBUtil.saveDFToDB(edges, "test.gudong_xinxi_1", SaveMode.Overwrite)
    
    
    val names = spark.sql("select distinct(gudong_name) as name from gudong where gudong_id is null")
//    HDFSUtil.saveDSToHDFS("/home/data_center/titan/gudong_name", names, SaveMode.Overwrite)
    DBUtil.saveDFToDB(names, "test.gudong_name", SaveMode.Overwrite)
    
    val compGudongName = spark.sql("select concat(company_id,',',gudong_name) from gudong where gudong_id is null")
//    HDFSUtil.saveDSToHDFS("/home/data_center/titan/gudong_xinxi_2", compGudongName, SaveMode.Overwrite)
    DBUtil.saveDFToDB(compGudongName, "test.gudong_xinxi_2", SaveMode.Overwrite)
    
  }
  
  
  def main(args: Array[String]): Unit = {
		  val spark = SparkSession.builder().master(ConfigUtil.master).appName("buildCompanyGraph").config("spark.sql.warehouse.dir", ConfigUtil.warehouse).config("spark.serializer", ConfigUtil.seralizer).getOrCreate()
		  zhengliGudongEdge(spark)
		  
  }
}