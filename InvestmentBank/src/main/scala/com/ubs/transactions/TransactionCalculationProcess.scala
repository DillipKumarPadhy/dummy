package com.ubs.spark.sparkjobs.assignment

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.{ SQLContext, SparkSession }
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types._
import com.ubs.spark.CommonUtil
import org.apache.spark.sql.catalyst.expressions.IsNaN

object TransactionCalculationProcess {

  var spark: SparkSession = null
  def main(args: Array[String]) {

    try {
      spark = CommonUtil.buildSparkSession()
      val spark1 = spark
      import spark1.implicits._

      //Read Input files
      val input_startOfDay_position_struct = ScalaReflection.schemaFor[CommonUtil.input_startOfDay_position].dataType.asInstanceOf[StructType]
      val input_transaction_struct = ScalaReflection.schemaFor[CommonUtil.input_transaction].dataType.asInstanceOf[StructType]
      var startOfDayPositionDF = CommonUtil.readFromFile("CSV", "src/main/resources/Input_StartOfDay_Positions.txt", spark1, input_startOfDay_position_struct)
      var transactionDF = CommonUtil.readFromFile("JSON", "src/main/resources/1537277231233_Input_Transactions.txt", spark1, input_transaction_struct)

      val calculateDF = calculateTransaction(startOfDayPositionDF, transactionDF)

      //write output
      val outputFile = "src/main/resources/Expected_EndOfDay_Positions.csv"
      CommonUtil.writeToCSV[CommonUtil.endOfDay_position](outputFile, calculateDF.as[CommonUtil.endOfDay_position])

    } catch {
      case t: Throwable => t.printStackTrace
    } finally {
      if (null != spark) {
        spark.stop()
      }
    }
  }

  def calculateTransaction(startOfDayDF: DataFrame, transactionDataframe: DataFrame): DataFrame = {
    val spark = CommonUtil.buildSparkSession()
    import spark.implicits._
    //transform to datatype
    var transactionDF = transactionDataframe.select('TransactionId.cast(StringType), 'Instrument.cast(StringType), 'TransactionType.cast(StringType),
      when(('TransactionQuantity.isNull || 'TransactionQuantity.isNaN), lit("0")).otherwise('TransactionQuantity).cast(LongType) as "TransactionQuantity")
      .withColumn("TransactionQuantity", when('TransactionQuantity.isNotNull, 'TransactionQuantity).otherwise(0))
      .withColumn("B", when('TransactionType.equalTo("B"), 'TransactionQuantity).otherwise(0))
      .withColumn("S", when('TransactionType.equalTo("S"), 'TransactionQuantity).otherwise(0))
    //evaluate sum by grouping Instrument                  
    transactionDF = transactionDF.groupBy('Instrument).agg(sum('B) as "totalB", sum('S) as "totalS")
    //transform to datatype
    var startOfDayDF1 = startOfDayDF.
      select('Instrument.cast(StringType), 'Account.cast(StringType), 'AccountType.cast(StringType),
        when(('Quantity.isNull || 'Quantity.isNaN), lit("0")).otherwise('Quantity).cast(LongType) as "Quantity")
      .withColumn("Quantity", when('Quantity.isNull, lit("0")).otherwise('Quantity).cast(LongType))

    var processDF = startOfDayDF1
      .join(transactionDF, Seq("Instrument"), "LeftOuter")
      .withColumn("Delta", when('AccountType.equalTo("E") && 'totalB.isNotNull && 'totalS.isNotNull, ('totalB - 'totalS))
        .otherwise(when('AccountType.equalTo("I") && 'totalB.isNotNull && 'totalS.isNotNull, ('totalS - 'totalB)).otherwise(0)).cast(LongType))
      .withColumn("Quantity", when('Quantity.isNotNull && 'Delta.isNotNull, ('Quantity + 'Delta)).otherwise('Quantity).cast(LongType))

    processDF = processDF.orderBy(col("totalS").asc_nulls_last, col("totalB").asc_nulls_last, col("AccountType").asc, col("Instrument").desc)
      .drop(col("totalB")).drop(col("totalS"))

    processDF
  }
}