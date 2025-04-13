package unstructuredLogs

import org.scalatest.funsuite.AnyFunSuite

class unstructuredDataSpec extends AnyFunSuite with testFunctions {

  test("Test log data processing") {

    import spark.implicits._

    // Input test data
    val testData = Seq(
      ("25/04/13 12:13:50 INFO MemoryStore: MemoryStore cleared"),
      ("25/04/13 12:13:50 INFO BlockManager: BlockManager stopped")
    ).toDF("logs")

    // Expected output
    val expectedData = Seq(
      ("25/04/13 12:13:50", "INFO", "MemoryStore", "MemoryStore cleared"),
      ("25/04/13 12:13:50", "INFO", "BlockManager", "BlockManager stopped")
    ).toDF("datetime", "information_type", "module_type", "message")

    // Process the test data using the same logic as in unstructuredData
    val processedData=unstructuredData.processUnstructredData(testData)

    // Assert that the processed data matches the expected data
    assert(processedData.collect() === expectedData.collect())

    spark.stop()
  }
}
