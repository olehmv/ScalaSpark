package hbase

import org.apache.hadoop.hbase.HConstants
import org.apache.spark.sql.execution.datasources.hbase.{HBaseRelation, HBaseTableCatalog}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}

object HBaseDriver {

  def main(args: Array[String]): Unit = {
//val conf=new SparkConf().setAppName("HBase").setMaster("local[*]")
//    val sc = new SparkContext(conf)
//    val session = new SparkSession(sc)
    val sqlContext: SQLContext= SparkSession
      .builder()
      .appName("HBase")
      .master("local[2]")
      .getOrCreate().sqlContext
    import sqlContext.implicits._
    val data = (1 to 254).map { i =>  HBaseRecord(i)}

//    sqlContext.sparkContext.parallelize(data).toDF.write.options(
//      Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "4",HBaseRelation.HBASE_CONFIGFILE-> "C:\\Users\\User\\IdeaProjects\\ScalaSpark\\hbase-site.xml"))
//      .format("org.apache.spark.sql.execution.datasources.hbase")
//      .save()

    val dataFrame:DataFrame = withCatalog(catalog,sqlContext)
    val rows: Array[Row] = dataFrame.take(250)
    rows.foreach(println)
  }
  def withCatalog(cat: String,sqlContext: SQLContext): DataFrame = {
    sqlContext
      .read
      .options(Map(HBaseTableCatalog.tableCatalog->cat,HBaseRelation.HBASE_CONFIGFILE-> "C:\\Users\\User\\IdeaProjects\\ScalaSpark\\hbase-site.xml"))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .load()
  }

  def catalog = s"""{
                   |"table":{"namespace":"default", "name":"table2"},
                   |"rowkey":"key",
                   |"columns":{
                   |"col0":{"cf":"rowkey", "col":"key", "type":"string"},
                   |"col1":{"cf":"cf1", "col":"col1", "type":"boolean"},
                   |"col2":{"cf":"cf2", "col":"col2", "type":"double"},
                   |"col3":{"cf":"cf3", "col":"col3", "type":"float"},
                   |"col4":{"cf":"cf4", "col":"col4", "type":"int"},
                   |"col5":{"cf":"cf5", "col":"col5", "type":"bigint"},
                   |"col6":{"cf":"cf6", "col":"col6", "type":"smallint"},
                   |"col7":{"cf":"cf7", "col":"col7", "type":"string"},
                   |"col8":{"cf":"cf8", "col":"col8", "type":"tinyint"}
                   |}
                   |}""".stripMargin

}
