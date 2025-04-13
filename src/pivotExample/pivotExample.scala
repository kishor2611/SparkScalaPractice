package pivotExample

import org.apache.spark.sql.catalyst.expressions.aggregate.First
import org.apache.spark.sql.connector.catalog.TableChange.First
import utilFunctions.commonFunctions._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions.{col, dense_rank, first, from_unixtime, lag, lit, regexp_replace, sum, timestamp_seconds, to_timestamp, unix_timestamp, when}


object pivotExample {

  def prepareData(spark:SparkSession):DataFrame={

    import spark.implicits._

    val data=Seq(
      ("John","maths","50"),
      ("John","Science","50"),
      ("joe","english","80"),
      ("joe","maths","50"),
      ("joe","Science","50")
    ).toDF("Name","Subject","Marks")

    data
  }

  def main(args:Array[String]):Unit={

    val spark=getSparkSession()
    import spark.implicits._

    val data=prepareData(spark)
    data.groupBy(col("Name")).pivot(col("Subject")).agg(first(col("marks"))).show

  }

}
