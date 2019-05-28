name := """InvestmentBank"""

version := "1.0"

scalaVersion := "2.11.8"

lazy val spark = "2.3.1"


// Spark related dependencies

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % spark,
  "org.apache.spark" %% "spark-sql" % spark,
  "org.scalatest" %% "scalatest" % "2.2.6",
  "com.holdenkarau" %% "spark-testing-base" % "2.1.0_0.6.0" % "test",
  "org.apache.spark" %% "spark-hive" % "2.2.1" % "test"
  
)
 parallelExecution in Test := false
fork in run := false
//added for git test