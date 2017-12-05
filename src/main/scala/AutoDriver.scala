// Import Spark SQL
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._
import scalafx.application.JFXApp

//https://www.jetbrains.com/help/idea/eclipse.html
//-Xms1336m -Xmx1336m
object AutoDriver extends JFXApp {


  val log = Logger.getLogger(getClass.getName)

  import org.apache.spark.sql.{DataFrame, Dataset, Row}

  val hiveSql = SparkSession.builder().appName("Auto bazara").master("local[*]").enableHiveSupport().getOrCreate()
  hiveSql.sparkContext.setLogLevel("WARN")
  val autoData0: DataFrame = hiveSql.read.format("com.databricks.spark.csv").options(Map("header" -> "true", "inferSchema" -> "true")).load("autos.csv")
  val brandList = autoData0.select("brand").distinct().collectAsList()
  var dataPairss = scala.collection.mutable.HashMap[String, Long]()
  var brandDataset: Dataset[Row] = null
  val brandIterator = brandList.iterator()
  while (brandIterator.hasNext) {
    var brandName: String = brandIterator.next().toString().replaceAll("\\]", "").replaceAll("\\[", "").trim
    if (brandName != "null") {
      brandDataset = autoData0.filter(autoData0.col("brand") === brandName)
      brandDataset.createOrReplaceTempView(brandName)
      dataPairss += brandName -> brandDataset.sqlContext.sql(s"select * from $brandName").count()
    }
  }
  stage = new JFXApp.PrimaryStage {

    import scalafx.scene.Scene

    title = "Auto Pie Chart "
    scene = new Scene (500,500){

      import scalafx.scene.chart.PieChart

      root = new PieChart {

        import scalafx.collections.ObservableBuffer

        title = "Auto Pie Chart"
        clockwise = false
        data = ObservableBuffer(dataPairss.toList.map { case (x, y) => PieChart.Data(x, y) })
      }
    }
  }
  hiveSql.stop()
}

