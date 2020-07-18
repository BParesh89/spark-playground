package org.example

import org. apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


//Reference --> https://medium.com/expedia-group-tech/working-with-json-in-apache-spark-1ecf553c2a8c

object readnestedjson {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("Nested Json Demo")
      .getOrCreate()
    // read json file
    val nestedJsonDF = spark.read.option("multiline",true)
      .json("/home/vagrant/Downloads/nestedsample.json")
      .withColumnRenamed("id","key")
    //select only two columns, explode batters.batter and create two new columns
    val batDF = nestedJsonDF
        .select(col("key"),explode(col("batters.batter")).alias("new_batter"))
      .select("key", "new_batter.*")
      .withColumnRenamed("id", "bat_id")
      .withColumnRenamed("type", "bat_type")

    batDF.show()




  }
}
