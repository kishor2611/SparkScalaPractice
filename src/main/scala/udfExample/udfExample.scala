package udfExample

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import utilFunctions.commonFunctions._

object udfExample {

  def capitalizeFirstletter(name:String):String={
    name.substring(0,1).toUpperCase() + name.substring(1).toLowerCase()
  }

  val capitalUDF= udf(capitalizeFirstletter _)

  def main(args:Array[String]):Unit={
    val spark= getSparkSession()
    import spark.implicits._

    val data=Seq(
      ("kishor"),
      ("isha"),
      ("keerthana")
    ).toDF("Name")

    data.select(capitalUDF(col("Name"))).show
  }

}
