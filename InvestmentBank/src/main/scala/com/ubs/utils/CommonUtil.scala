package com.ubs.spark

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object CommonUtil {
  case class input_startOfDay_position(Instrument: Option[String], Account: Option[String], AccountType: Option[String], Quantity: Option[Long])
  case class input_transaction(TransactionId: Option[Int], Instrument: Option[String], TransactionType: Option[String], TransactionQuantity: Option[Long])
  case class endOfDay_position(Instrument: Option[String], Account: Option[String], AccountType: Option[String], Quantity: Option[Long], Delta: Option[Long])

  def buildSparkSession(): SparkSession = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("InvestmentBank")
    val sparkSession: SparkSession = SparkSession.builder.config(sparkConf).getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")
    sparkSession
  }

  def readFromFile(fileType: String, filePath: String, spark: SparkSession, schema: StructType): DataFrame = {
    if (filePath == null || (filePath != null && filePath.trim() == "")) {
      throw new RuntimeException("filePath is null or blank")
    } else if (spark == null) {
      throw new RuntimeException("spark session is null")
    } else if ("CSV".equalsIgnoreCase(fileType)) {
      var csvDF = spark.read
        .format("csv")
        // .schema(schema)
        .option("header", true)
        .option("delimiter", ",")
        .option("inferSchema", "true")
        .load(filePath)
      csvDF
    } else if ("JSON".equalsIgnoreCase(fileType)) {
      val rawJson = spark.sparkContext.wholeTextFiles(filePath).map(tuple => tuple._2.replace("\n", "").trim)
      var jsonDF = spark.read
        // .schema(schema).option("inferSchema", "false")
        .json(rawJson)
      jsonDF
    } else {
      throw new RuntimeException("File type  is not specified correctly ")
    }
  }

  def writeToCSV[T](filePath: String, ds: Dataset[T]) = {
    if (filePath == null || (filePath != null && filePath.trim() == "")) {
      throw new RuntimeException("filePath is null or blank")
    } else {
      ds.coalesce(1).write.mode("Overwrite")
        .format("csv")
        .option("header", true)
        .option("delimiter", ",")
        .save(filePath)
    }
  }

}