package org.example

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.log4j.{Logger, LogManager}

object sparkdemo {

  def main(args: Array[String]): Unit = {

    val logger = LogManager.getLogger(getClass.getName)
    //create spark session
    val spark = SparkSession.builder
      //.master("local[*]")
      .appName("invalidcustomer")
      .getOrCreate()
    logger.info("Spark Session created!")
    val orderschema = StructType(Array(
      StructField("order_id",IntegerType, true),
      StructField("order_date", StringType, true),
      StructField("order_customer_id", IntegerType, true),
      StructField("order_status", StringType, true)))

    val orderDf = spark.read
      .option("sep",",")
      .schema(orderschema)
      .csv("/opt/data-master/retail_db/orders")
    logger.info("Reading ordrers file done")

    val customerschema = StructType(Array(StructField("customer_id", IntegerType, true),
      StructField("customer_fname", StringType, true),
      StructField("customer_lname", StringType, true))
      )

    val customerDf = spark.read
      .option("sep",",")
      .schema(customerschema)
      .csv("/opt/data-master/retail_db/customers")
    logger.info("Reading customers file done")

    val distinctCustOrder = orderDf.select("order_customer_id")
      .distinct()
      .withColumnRenamed("order_customer_id","customer_id")

    val invalidCustDF = customerDf.join(distinctCustOrder,Seq("customer_id"),"leftanti")
    invalidCustDF.show()
    logger.info("Program end")

  }

}
