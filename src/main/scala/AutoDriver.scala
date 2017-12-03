// Import Spark SQL
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, TimestampType}
//https://www.jetbrains.com/help/idea/eclipse.html
object AutoDriver {

  def main(args: Array[String]): Unit = {
    val hiveSql = SparkSession.builder().appName("Auto bazara").master("local[*]").enableHiveSupport().getOrCreate()
    import hiveSql.implicits._
    import org.apache.spark.sql.types.{DateType, DoubleType, StringType, StructField, StructType}
    val aschema = StructType(Array(
      StructField("dateCrawled", DataTypes.TimestampType),
      StructField("name", StringType),
      StructField("seller", StringType),
      StructField("offerType", StringType),
      StructField("price", DoubleType),
      StructField("abtest",StringType),
      StructField("vehicleType",StringType),
      StructField("yearOfRegistration",DoubleType),
      StructField("gearbox",StringType),
      StructField("powerPS",DoubleType),
      StructField("model",StringType),
      StructField("kilometer",DoubleType),
      StructField("monthOfRegistration",DoubleType),
      StructField("fuelType",StringType),
      StructField("brand",StringType),
      StructField("notRepairedDamage",StringType),
      StructField("dateCreated",DataTypes.TimestampType),
      StructField("nrOfPictures",DoubleType),
      StructField("postalCode",DoubleType),
      StructField("lastSeen",DataTypes.TimestampType)

    ))

    val autoData2 = hiveSql.read.schema(aschema).options(Map("header"->"true","dateCrawled"->"yyyy-MM-dd HH:mm:ss","dateCreated"->"yyyy-MM-dd HH:mm:ss","lastSeen"->"yyyy-MM-dd HH:mm:ss")).csv("autos.csv")
    autoData2.show()


  }
}
