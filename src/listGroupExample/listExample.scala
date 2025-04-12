package listGroupExample

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


object listExample {

  def prepareList():DataFrame={

    val spark=SparkSession.builder.appName("example")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val data=Seq(
      (1,"[pizza, burger, fries]"),
      (2,"[pizza, burger]"),
      (3,"[pizza, fries]"),
      (4,"[burger, fries]"),
      (5,"[pizza, burger, fries]")
    ).toDF("id","items")

    data
  }

  def main(args: Array[String]): Unit={

    val spark=SparkSession.builder.appName("example")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._
    val input_data=prepareList()
    val cleaned_data=input_data.withColumn("items", regexp_replace($"items", "\\[|\\]", ""))
    val exploded_data=cleaned_data.withColumn("item", explode(split($"items", ",")))
    val grouped_data=exploded_data.groupBy("item").agg(count("*").as("count"))
    grouped_data.show()

  }
}


