package utilFunctions

import org.apache.spark.sql.SparkSession

object commonFunctions {

  def getSparkSession():SparkSession={
    SparkSession.builder().appName("practice").master("local[*]").getOrCreate()

  }

}
