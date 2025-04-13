package timeSeriesProblem

import utilFunctions.commonFunctions._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions.{col, dense_rank, from_unixtime, lag, lit, regexp_replace, sum, timestamp_seconds, to_timestamp, unix_timestamp, when}

object timeSeries {

  def prepareData(spark:SparkSession):DataFrame={

    import spark.implicits._
    val data=Seq(("2018-01-01 11:00:00Z","u1"),
      ("2018-01-01 12:00:00Z","u1"),
      ("2018-01-01 11:00:00Z","u2"),
      ("2018-01-02 11:00:00Z","u2"),
      ("2018-01-01 12:15:00Z","u1"),
      ("2018-01-01 12:20:00Z","u1"),
      ("2018-01-01 13:20:00Z","u1")
    ).toDF("timestamp","userid")

//    val data=Seq(
//      ("2018-08-10 10:30:00Z","u1")
//    ).toDF("timestamp","userid")
    data
  }

  def main(args:Array[String]):Unit={

    val spark=getSparkSession()
    val seriesData=prepareData(spark)
    seriesData.show
    import spark.implicits._
    val formattedData=seriesData.withColumn("timestamp",unix_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss'Z'"))
    formattedData.show
    val timeDiffData = formattedData.withColumn("prevRow",lag(col("timestamp"),1).
      over(Window.partitionBy(col("userid")).orderBy(col("timestamp").asc))).
      withColumn("timeDiff",col("timestamp") -col("prevRow"))
      .withColumn("samesession",when(col("timeDiff").isNull || col("timeDiff")>(1800),lit(1)).otherwise(lit(0)))
    val windowSpec2 = Window.partitionBy("userid").orderBy("timestamp").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    val dfWithSessionId = timeDiffData.withColumn("session_id", sum($"samesession").over(windowSpec2))
    val dfWithSessionIdString = dfWithSessionId.withColumn("session_id", functions.concat($"userid", lit("_s"), $"session_id"))
    val dfWithOriginalTimestamp = dfWithSessionIdString.withColumn("timestamp", from_unixtime($"timestamp", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
    val result = dfWithOriginalTimestamp.select("timestamp", "userid", "session_id")
    result.show(truncate = false)

  }

}
