package unstructuredLogs

import org.apache.spark.sql.functions.{col, regexp_extract}
import udfExample.udfExample.capitalUDF
import utilFunctions.commonFunctions.getSparkSession

object unstructuredData {

  def main(args:Array[String]):Unit={
    val spark= getSparkSession()
    import spark.implicits._

    val data=Seq(
      ("25/04/13 12:13:50 INFO MemoryStore: MemoryStore cleared"),
      ("25/04/13 12:13:50 INFO BlockManager: BlockManager stopped"),
      ("25/04/13 12:13:50 INFO BlockManagerMaster: BlockManagerMaster stopped"),
      ("25/04/13 12:13:50 INFO OutputCommitCoordinator$OutputCommitCoordinatorEndpoint: OutputCommitCoordinator stopped!"),
      ("25/04/13 12:13:50 INFO SparkContext: Successfully stopped SparkContext"),
      ("25/04/13 12:13:50 INFO ShutdownHookManager: Shutdown hook called"),
      ("25/04/13 12:13:50 INFO ShutdownHookManager: Deleting directory /private/var/folders/_x/1gtpccvx0y7dkx8kl1_v_2bw0000gn/T/spark-d999091b-5931-4a53-8c9d-f05cc8a668df")
    ).toDF("logs")

    val processedData = data
      .withColumn("datetime", regexp_extract(col("logs"), """^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})""", 1))
      .withColumn("information_type", regexp_extract(col("logs"), """\s(INFO|ERROR|WARN|DEBUG)\s""", 1))
      .withColumn("module_type", regexp_extract(col("logs"), """\s(INFO|ERROR|WARN|DEBUG)\s(.*?):""", 2))
      .withColumn("message", regexp_extract(col("logs"), """\s(INFO|ERROR|WARN|DEBUG)\s.*?:\s(.*)""", 2))

    processedData.select("datetime", "information_type", "module_type", "message").show(false)
  }

}
