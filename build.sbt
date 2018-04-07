name := "ScalaSpark"

version := "0.1"

scalaVersion := "2.11.0"

resolvers += "Hortonworks Repository" at "http://repo.hortonworks.com/content/repositories/releases/"



libraryDependencies +="org.apache.spark" % "spark-core_2.11" % "2.2.0"
// https://mvnrepository.com/artifact/org.apache.spark/spark-hive
libraryDependencies += "org.apache.spark" % "spark-hive_2.11" % "2.2.0"
// https://mvnrepository.com/artifact/org.scalafx/scalafx
libraryDependencies += "org.scalafx" %% "scalafx" % "8.0.144-R12"
// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.2.0"
// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-twitter
libraryDependencies += "org.apache.spark" %% "spark-streaming-twitter" % "1.6.0"

libraryDependencies ++= Seq(
  "com.hortonworks" % "shc-core" % "1.1.1-2.1-s_2.11"
)

