// Import Spark SQL
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

//https://www.jetbrains.com/help/idea/eclipse.html
//-Xms1336m -Xmx1336m
object AutoDriver {

  def main(args: Array[String]): Unit = {
    val log = Logger.getLogger(getClass.getName)
    import org.apache.spark.sql.{DataFrame, Dataset, Row}
    val hiveSql = SparkSession.builder().appName("Auto bazara").master("local[*]").enableHiveSupport().getOrCreate()
    hiveSql.sparkContext.setLogLevel("WARN")
    val autoData0: DataFrame = hiveSql.read.format("com.databricks.spark.csv").options(Map("header" -> "true", "inferSchema" -> "true", "dateCrawled" -> "yyyy-MM-dd HH:mm:ss", "dateCreated" -> "yyyy-MM-dd 45", "lastSeen" -> "yyyy-MM-dd HH:mm:ss")).load("autos.csv")
    val brandList = autoData0.select("brand").distinct().collectAsList()
    print(brandList)
    var brandDataset: Dataset[Row] = null
    val brandIterator = brandList.iterator()
    while (brandIterator.hasNext) {
      var brandName: String = brandIterator.next().toString().replaceAll("\\]", "").replaceAll("\\[", "").trim
      if (brandName != "null") {
        brandDataset = autoData0.filter(autoData0.col("brand") === brandName)
        brandDataset.createOrReplaceTempView(brandName)
        brandDataset.sqlContext.sql(s"select * from $brandName").show()
      }
    }

    hiveSql.stop()
  }
}

