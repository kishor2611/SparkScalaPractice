package scala

import org.apache.spark.sql.SparkSession

trait testFunctions {

  val spark = SparkSession.builder()
    .appName("unstructuredDataTest")
    .master("local[*]")
    .getOrCreate()
}
