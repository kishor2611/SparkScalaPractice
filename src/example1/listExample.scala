package example1

import org.apache.spark.sql.SparkSession.
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


object(listExample){

  def prepareList():dataFrame={

    val data=Seq(
      (1,"[pizza, burger, fries]"),
      (2,"[pizza, burger]"),
      (3,"[pizza, fries]"),
      (4,"[burger, fries]"),
      (5,"[pizza, burger, fries]")

      data.toDF("id","items")
    )
  }

  def main(args: Array[String]): Unit={

  }
}


