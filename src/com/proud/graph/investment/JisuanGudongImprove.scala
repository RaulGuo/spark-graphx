package com.proud.graph.investment

import org.apache.spark.sql.Dataset
import com.proud.ark.db.DBUtil
import org.apache.spark.sql.SparkSession
import com.proud.ark.config.ConfigUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.sql.functions._
import com.google.gson.Gson
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.SaveMode
import com.proud.ark.data.HDFSUtil
import com.proud.ark.config.DBName
import com.proud.ark.config.GlobalVariables

/**
 * 基本思路：
 * 有没有可能将企业对应的股东换成顶点的属性
spark-submit --master local[*] --class com.proud.graph.investment.JisuanGudongImprove --driver-memory 25g --jars /home/data_center/dependency/mysql-connector-java.jar,/home/data_center/dependency/gson.jar,/home/data_center/dependency/ArkUtil-0.0.1-SNAPSHOT.jar /home/data_center/dependency/graphx-0.0.1-SNAPSHOT.jar
spark-submit --master spark://bigdata01:7077 --class com.proud.graph.investment.JisuanGudongImprove --executor-memory 10g --jars /home/data_center/dependency/mysql-connector-java.jar,/home/data_center/dependency/gson.jar,/home/data_center/dependency/ArkUtil-0.0.1-SNAPSHOT.jar /home/data_center/dependency/graphx-0.0.1-SNAPSHOT.jar
nohup spark-submit --master local[*] --class com.proud.graph.investment.JisuanGudongImprove --driver-memory 25g --jars /home/data_center/dependency/mysql-connector-java.jar,/home/data_center/dependency/gson.jar,/home/data_center/dependency/ArkUtil-0.0.1-SNAPSHOT.jar /home/data_center/dependency/graphx-0.0.1-SNAPSHOT.jar &
spark-shell --master spark://bigdata01:7077 --jars /home/data_center/dependency/mysql-connector-java.jar,/home/data_center/dependency/gson.jar,/home/data_center/dependency/ArkUtil-0.0.1-SNAPSHOT.jar
测试数据的例子:
select * from gudong_xinxi x1 inner join gudong_xinxi x2 on x1.gudong_id = x2.company_id inner join gudong_xinxi x3 on x2.gudong_id = x3.company_id inner join gudong_xinxi x4 on x3.gudong_id = x4.company_id where x1.company_id in (437,1631082);
在simp中准备一个包含四级股东的数据：
delete from gudong_xinxi_simp where company_id = 437;
insert into gudong_xinxi_simp select * from gudong_xinxi where company_id = 437;
delete from gudong_xinxi_simp where company_id = 1631082;
insert into gudong_xinxi_simp select * from gudong_xinxi where company_id = 1631082;
delete from gudong_xinxi_simp where company_id in (1593887, 1630523);
insert into gudong_xinxi_simp select * from gudong_xinxi where company_id in (1593887, 1630523);
delete from gudong_xinxi_simp where company_id in (
select gudong_id from gudong_xinxi where company_id in (1593887, 1630523)
);
insert into gudong_xinxi_simp select * from gudong_xinxi where company_id in (
select gudong_id from gudong_xinxi where company_id in (1593887, 1630523)
);
 */

object JisuanGudongImprove {
  val resultTable = "test.company_stock_relationship"
//  type EdgeAttr = (String, String, Long)
  //边的属性
  
  
  case class VertexBasic(company_id:Long, company_name:String, company_md5:String)
  case class VertexInvest(company_id:Long, attrs:Array[GudongAttr])
  //股东的关系，顶点的属性是企业的名称和md5
  def loadGudongInvGraph(gudongDS:Dataset[Gudong], spark:SparkSession):Graph[VertexAttr, Int] = {
    import spark.implicits._
    //所有的顶点
    val verticesDS = gudongDS.flatMap(x => {
      if(x.gudong_id.isDefined){
        Array(VertexBasic(x.company_id, x.company_name, x.company_md5), VertexBasic(x.gudong_id.getOrElse(-1), x.gudong_name, x.gudong_md5))
      }else{
        Array(VertexBasic(x.company_id, x.company_name, x.company_md5))
      }
    }).distinct()
    
    
    //只包含带股东的所有企业以及股东信息
    val compVertices = gudongDS.groupByKey { x => x.company_id }.mapGroups{
      case(id, it) => {
        val gudontAttrs = it.map { x => GudongAttr(x.gudong_name, x.gudong_md5) }.toArray
        VertexInvest(id, gudontAttrs)
      }
    }
    
    //将顶点的属性关联上去
    val df = verticesDS.join(compVertices, Seq("company_id"), "left_outer")
    
    val vertices = df.mapPartitions{ _.map{ 
      row => {
       val id = row.getAs[Long]("company_id")
       val name = row.getAs[String]("company_name")
       val md5 = row.getAs[String]("company_md5")
       val attrRow = row.getAs[Seq[GenericRowWithSchema]]("attrs")
       if(attrRow == null){
         (id, VertexAttr(name, md5, Seq()))
       }else{
         val attr = attrRow.map { x => {
           GudongAttr(x.getAs("name"), x.getAs("company_id"))
         } }
         (id, VertexAttr(name, md5, attr))
       }
    }}}.rdd
    
    //整理边的信息，边的属性
    val edges = gudongDS.filter(x => x.gudong_id.isDefined).mapPartitions { _.map {
      g => {
        Edge(g.company_id, g.gudong_id.getOrElse(-1L), 1)
    }}}.rdd
    
    val graph = Graph(vertices, edges)
    
    graph
  }
  
  val gson = new Gson
  
  case class Gudong(company_id:Long, company_name:String, company_md5:String, gudong_id:Option[Long], gudong_name:String, gudong_md5:String)
  case class GudongAttr(name:String, company_id:String)
  case class VertexAttr(company_name:String, company_md5:String, gudongs:Seq[GudongAttr])
  
//  case class Stock(name:String, company_id:String)
  case class YijiGudong(name:String, company_id:String, children: Array[GudongAttr])
  case class ErjiGudong(name:String, company_id:String, children: Array[YijiGudong])
  case class SanjiGudong(name:String, company_id:String, children: Array[ErjiGudong])
  case class SijiGudong(name:String, company_id:String, children: Array[SanjiGudong])
  
  def transYijiGudong(attr: (VertexId, VertexAttr)) = {
    val gudongArray = attr._2.gudongs.toArray
    val md5 = attr._2.company_md5
    val name = attr._2.company_name
    new YijiGudong(name, md5, gudongArray)
  }
  
  def transErjiGudong(attr: (VertexId, (VertexAttr, Array[(VertexId, VertexAttr)]))) = {
    val md5 = attr._2._1.company_md5
    val name = attr._2._1.company_name
    val yijiArray:Array[YijiGudong] = attr._2._2.map(transYijiGudong(_))
    //除了图中能传递过来的带二级股东的顶点，还需要补充属性中本来就有的以及股东。需要先获取带下一级股东的顶点，排除掉后将没有下一级股东的节点补充到以及股东中
    //先获取到所有的带下一级股东的顶点ID
    val idExists = attr._2._2.map(x => x._2.company_md5).toSet
    //将过滤掉的顶点的属性进行转换
    val yijiExtraGudongNoRelate = attr._2._1.gudongs.filter { x => !idExists.contains(x.company_id) }.map { x => new YijiGudong(x.name, x.company_id, Array()) }
    
    new ErjiGudong(name, md5, yijiArray ++ yijiExtraGudongNoRelate)
  }
  
  def transSanjiGudong(attr: (VertexId, (VertexAttr, Array[(VertexId, (VertexAttr, Array[(VertexId, VertexAttr)]))]))) = {
    val md5 = attr._2._1.company_md5
    val name = attr._2._1.company_name
    val erjiArray = attr._2._2.map(transErjiGudong(_))
    
    val idExists = attr._2._2.map(x => x._2._1.company_md5)
    val yijiExtraGudongNoRelate = attr._2._1.gudongs.filter { x => !idExists.contains(x.company_id) }.map { x => new ErjiGudong(x.name, x.company_id, Array()) }
    
    new SanjiGudong(name, md5, erjiArray ++ yijiExtraGudongNoRelate)
  }
  
  def transSijiGudong(attr: (VertexId, (VertexAttr, Array[(VertexId, (VertexAttr, Array[(VertexId, (VertexAttr, Array[(VertexId, VertexAttr)]))]))]))) = {
    val md5 = attr._2._1.company_md5
    val name = attr._2._1.company_name
    val sanjiArray = attr._2._2.map(transSanjiGudong(_))
    
    val idExists = attr._2._2.map(x => x._2._1.company_md5)
    val yijiExtraGudongNoRelate = attr._2._1.gudongs.filter { x => !idExists.contains(x.company_id) }.map { x => new SanjiGudong(x.name, x.company_id, Array()) }
    
    new SijiGudong(name, md5, sanjiArray ++ yijiExtraGudongNoRelate)
  }
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("GudongTuJisuan").master(ConfigUtil.master).config("spark.sql.warehouse.dir", ConfigUtil.warehouse).getOrCreate()  
    val sc = spark.sparkContext
    import spark.implicits._
    val gudongDS = HDFSUtil.getDSFromHDFS(GlobalVariables.hdfsGudongXinxi, spark).select("company_id", "company_md5", "company_name", "gudong_name", "gudong_id", "gudong_md5").as[Gudong].persist()
  
    //获得图，图的顶点属性是顶点的name, md5以及其对应的所有股东
    val graph = loadGudongInvGraph(gudongDS, spark).persist()
    gudongDS.unpersist()
//    val yijiJson:RDD[(Long, String)] = graph.vertices.map(vertex => {
//      val yiji = transYijiGudong(vertex)
//      val yijiStr = gson.toJson(yiji)
//      (vertex._1, yijiStr)
//    })
//    DBUtil.saveDFToDB(yijiJson.toDF("company_id", "relationship"), "test.gudong_yiji")
    
    //收集邻居顶点属性，RDD中的元素是顶点对应的所有的邻居节点
    val vertexWithErjiNeighbourRDD:VertexRDD[Array[(VertexId, VertexAttr)]] = graph.collectNeighbors(EdgeDirection.Out)
    val erjiTouziGraph = graph.outerJoinVertices(vertexWithErjiNeighbourRDD){
      case(id, orgVerAttr, neighbours) => (orgVerAttr, neighbours.getOrElse(Array()))
    }.persist()
    
    graph.unpersist()
//    val erjiJson:RDD[(Long, String)] = erjiTouziGraph.vertices.map(vertex => {
//      val erji = transErjiGudong(vertex)
//      val erjiStr = gson.toJson(erji)
//      (vertex._1, erjiStr)
//    })
//    DBUtil.saveDFToDB(erjiJson.toDF("company_id", "relationship"), "test.gudong_erji", SaveMode.Overwrite)
    
    val vertexWithSanjiNeighbourRDD = erjiTouziGraph.collectNeighbors(EdgeDirection.Out)
    val sanjiTouziGraph = graph.outerJoinVertices(vertexWithSanjiNeighbourRDD){
      case(id, orgVerAttr, neighbours) => (orgVerAttr, neighbours.getOrElse(Array()))
    }.persist()
    erjiTouziGraph.unpersist()
    
    val sanjiJson:RDD[(Long, String)] = sanjiTouziGraph.vertices.map(vertex => {
      val sanji = transSanjiGudong(vertex)
      val sanjiStr = gson.toJson(sanji)
      (vertex._1, sanjiStr)
    })
//    DBUtil.saveDFToDB(sanjiJson.toDF("company_id", "relationship"), "test.gudong_sanji", SaveMode.Overwrite)
    
    
    val vertexWithSijiNeighbourRDD = sanjiTouziGraph.collectNeighbors(EdgeDirection.Out)
    val sijiTouziGraph = graph.outerJoinVertices(vertexWithSijiNeighbourRDD){
      case(id, orgVerAttr, neighbours) => (orgVerAttr, neighbours.getOrElse(Array()))
    }.persist()
    sanjiTouziGraph.unpersist()
//    val erjiTuoziRdd = graph.aggregateMessages[Array[(VertexId, VertexAttr)]](ctx => ctx.sendToSrc(Array((ctx.dstId, ctx.dstAttr))), _++_, TripletFields.Dst)
    val sijiJson:RDD[(Long, String)] = sijiTouziGraph.vertices.mapPartitions(_.map {
      vertex => {
        val siji = transSijiGudong(vertex)
        val sijiStr = gson.toJson(siji)
        (vertex._1, sijiStr)
    }})
    
    val df = sijiJson.toDF("company_id", "relationship").persist()
//    df.write.json("file:///home/data_center/gudong")
//    HDFSUtil.saveDSToHDFS("/home/data_center/gudongGraph", df, SaveMode.Overwrite)
    DBUtil.truncate(resultTable)
    DBUtil.saveDFToDB(df, resultTable)
  }
  
  
  
  
  
}