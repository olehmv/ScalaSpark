// Import Spark SQL
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
//https://www.jetbrains.com/help/idea/eclipse.html
object AutoDriver {


  object AutoDriver {

    def main(args: Array[String]): Unit = {
      val log = Logger.getLogger(getClass.getName)
      import org.apache.spark.sql.{DataFrame, Dataset, Row}
      import org.apache.spark.sql.types.{DateType, DoubleType, StringType, StructField, StructType}

      import scala.collection.immutable.HashMap
      val hiveSql = SparkSession.builder().appName("Auto bazara").master("local[*]").enableHiveSupport().getOrCreate()


      val autoData0: DataFrame = hiveSql.read.format("com.databricks.spark.csv").options(Map("header" -> "true", "inferSchema" -> "true", "dateCrawled" -> "yyyy-MM-dd HH:mm:ss", "dateCreated" -> "yyyy-MM-dd 45", "lastSeen" -> "yyyy-MM-dd HH:mm:ss")).load("autos.csv").cache()
      val brandList = autoData0.select("brand").distinct().collectAsList();
      val brandIterator = brandList.iterator()
      var brandMap = scala.collection.mutable.HashMap[String, Dataset[Row]]()
      var rowCount: Long = 0
      var mapValueCount: Long = 0
      while (brandIterator.hasNext) {
        var brandName: String = brandIterator.next().toString().replaceAll("\\]", "").replaceAll("\\[", "").trim
        var brandDataset: Dataset[Row] = autoData0.filter(autoData0.col("brand") === brandName)
        brandMap += brandName -> brandDataset
        brandMap(brandName).show(1)
        rowCount = brandDataset.count()
        mapValueCount = brandMap(brandName).count()
        if (rowCount != mapValueCount) {
          print("Warning rowCount != mapValueCount Warning rowCount != mapValueCount Warning rowCount != mapValueCount")
          log.info("Warning rowCount != mapValueCount Warning rowCount != mapValueCount Warning rowCount != mapValueCount")
        }
      }
    }
  }
}