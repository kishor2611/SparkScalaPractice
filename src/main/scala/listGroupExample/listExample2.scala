package listGroupExample

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import utilFunctions.commonFunctions.getSparkSession

object listExample2 {

  def preparedata(spark:SparkSession):DataFrame={

    import spark.implicits._

    val data=Seq(
      (1,"['pizza','burger']"),
      (2,"['pizza','fries']"),
      (3,"['pizza','shakes']"),
      (4,"['pizza','fries','shakes']")
    ).toDF("id","items")

    data
  }


  def main(args:Array[String]):Unit={

    val spark=getSparkSession()
    import spark.implicits._

    val inputData = preparedata(spark)
    val cleanData = inputData.withColumn("items", regexp_replace(col("items"),"\\[|\\]","") )
    val splitData = cleanData.withColumn("items",split(col("items"),","))
    val explodeData = splitData.withColumn("item",explode(col("items")))
    val groupedData = explodeData.groupBy("item").agg(count(col("item")))

    groupedData.show


  }


}
