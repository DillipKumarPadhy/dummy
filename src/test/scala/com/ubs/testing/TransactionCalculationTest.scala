import org.scalatest._
import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.DataFrame
import com.ubs.spark.CommonUtil
import org.apache.spark.sql.functions._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types._
import com.ubs.spark.sparkjobs.assignment.TransactionCalculationProcess
import com.holdenkarau.spark.testing._
import org.scalatest.FunSuite
import com.holdenkarau.spark.testing.{ RDDComparisons }
import org.scalatest.fixture.FlatSpec
class TransactionCalculationTest extends FunSuite with DatasetSuiteBase {
  test("test for valid inputs ") {
    val inputFileLocation1 = "src/main/resources/Test/Input_StartOfDay_Positions.txt"
    val inputFileLocation2 = "src/main/resources/Test/Input_Transactions.txt"
    val expectedOutputFileLocation = "src/main/resources/Test/Expected_Output.csv"

    excuteTestBlock("CSV", expectedOutputFileLocation, inputFileLocation1, inputFileLocation2)
  }
  test("test for invalid inputs ") {
    val inputFileLocation1 = "src/main/resources/Test/Input_StartOfDay_Positions_1.txt"
    val inputFileLocation2 = "src/main/resources/Test/Input_Transactions_1.txt"
    val expectedOutputFileLocation = "src/main/resources/Test/Expected_Output_1.csv"

    excuteTestBlock("CSV", expectedOutputFileLocation, inputFileLocation1, inputFileLocation2)
  }
  def excuteTestBlock(fileFormat: String, path: String, inputFileLocation1: String, inputFileLocation2: String): Unit = {
    val spark = CommonUtil.buildSparkSession()
    import spark.implicits._

    val input_startOfDay_position_struct = ScalaReflection.schemaFor[CommonUtil.input_startOfDay_position].dataType.asInstanceOf[StructType]
    val input_transaction_struct = ScalaReflection.schemaFor[CommonUtil.input_transaction].dataType.asInstanceOf[StructType]
    var startOfDayPositionDF = CommonUtil.readFromFile("CSV", inputFileLocation1, spark, input_startOfDay_position_struct)
    var transactionDF = CommonUtil.readFromFile("JSON", inputFileLocation2, spark, input_transaction_struct)
    val resultDataframe = TransactionCalculationProcess.calculateTransaction(startOfDayPositionDF, transactionDF).as[CommonUtil.endOfDay_position]

    val expected_struct = ScalaReflection.schemaFor[CommonUtil.endOfDay_position].dataType.asInstanceOf[StructType]
    var exceptedDataframe = spark.read
        .format("csv")
        .schema(expected_struct)
        .option("header", true)
        .option("delimiter", ",")
        .option("inferSchema", "true")
        .load(path) 
        .as[CommonUtil.endOfDay_position]

    assertDatasetEquals(exceptedDataframe, resultDataframe)
  }
}
