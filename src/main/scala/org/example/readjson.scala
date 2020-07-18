package org.example

import org.apache.spark.sql.SparkSession

object readjson {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Simple Json read")
      .getOrCreate()

    val jsonDF = spark.read.option("multiline", true)
      .json("/home/vagrant/Downloads/unece.json")

    jsonDF.show(5, false)

  }

}
