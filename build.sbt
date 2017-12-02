name := "ScalaSpark"

version := "0.1"

scalaVersion := "2.11.0"

libraryDependencies +="org.apache.spark" % "spark-core_2.11" % "2.2.0"
// https://mvnrepository.com/artifact/org.apache.spark/spark-hive
libraryDependencies += "org.apache.spark" % "spark-hive_2.11" % "2.2.0"
