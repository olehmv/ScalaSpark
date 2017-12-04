// Import Spark SQL
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
//https://www.jetbrains.com/help/idea/eclipse.html

object AutoDriver {

  def main(args: Array[String]): Unit = {
    val log = Logger.getLogger(getClass.getName)
    import org.apache.spark.sql.{DataFrame, Dataset, Row}
    import org.apache.spark.sql.types.{DateType, DoubleType, StringType, StructField, StructType}

    import scala.collection.immutable.HashMap
    val hiveSql = SparkSession.builder().appName("Auto bazara").master("local[*]").enableHiveSupport().getOrCreate()
//    hiveSql.sparkContext.setLogLevel("WARN")

    val autoData0: DataFrame = hiveSql.read.format("com.databricks.spark.csv").options(Map("header" -> "true", "inferSchema" -> "true", "dateCrawled" -> "yyyy-MM-dd HH:mm:ss", "dateCreated" -> "yyyy-MM-dd 45", "lastSeen" -> "yyyy-MM-dd HH:mm:ss")).load("autos.csv").cache()
    val brandList = autoData0.select("brand").distinct().collectAsList();
    var brandDataset: Dataset[Row]=null
    val brandIterator = brandList.iterator()
    while (brandIterator.hasNext) {
      var brandName: String = brandIterator.next().toString().replaceAll("\\]", "").replaceAll("\\[", "").trim
      if(brandName==null){
       print(brandName)
        return
      }
      brandDataset= autoData0.filter(autoData0.col("brand") === brandName)
      if(brandDataset==null){
        brandDataset.show(1)
      }
      brandDataset.registerTempTable(brandName)
//      print(brandDataset.select(s"select count(brand) from $brandName"))
    }
    brandDataset.select("SELECT kilometer FROM volvo").show(2)
//    autoData0.filter(autoData0.col("brand") === "jaguar").show()
  }
}
//17/12/04 17:21:12 INFO SparkSqlParser: Parsing command: jaguar
//17/12/04 17:21:12 INFO SparkSqlParser: Parsing command: daihatsu
//17/12/04 17:21:12 INFO SparkSqlParser: Parsing command: mitsubishi
//17/12/04 17:21:12 INFO SparkSqlParser: Parsing command: null
//17/12/04 17:21:12 INFO SparkSqlParser: Parsing command: lada
