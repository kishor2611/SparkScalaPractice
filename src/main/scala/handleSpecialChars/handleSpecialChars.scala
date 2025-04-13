package handleSpecialChars

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import utilFunctions.commonFunctions.getSparkSession

object handleSpecialChars {

  def prepareData(spark:SparkSession):DataFrame={

    import spark.implicits._
    val data=Seq(
      (1,"Kishor","7500000"),
      (6,"#",""),
      (5,"@","NA"),
      (4,"!","%"),
      (3,"asbd","123213")
    ).toDF("id","Name","Salary")

    data
  }

  def main(args:Array[String]):Unit={

    val spark=getSparkSession()
    import spark.implicits._
    val inputData=prepareData(spark)
    val specialChars = List("@","#","!","%","$","^")

    val cleanedData=inputData.columns.foldLeft(inputData)((currDf,colName)=>
      currDf.withColumn(colName,when(col(colName).isin(specialChars: _*) ||
        col(colName).isNull || col(colName).isin("NA") || col(colName).isin("")
        ,lit(null) ).otherwise(col(colName)))
    )

    cleanedData.show()

  }

}
