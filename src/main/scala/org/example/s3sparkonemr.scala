package org.example

import java.io.File

//import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object s3sparkonemr {
  def main(args: Array[String]): Unit = {

    //create spark session
    val spark = SparkSession.builder
      //.master("local[*]")
      .appName("invalidCustomer")
      .getOrCreate()

    val orderschema = StructType(Array(
      StructField("order_id",IntegerType, true),
      StructField("order_date", StringType, true),
      StructField("order_customer_id", IntegerType, true),
      StructField("order_status", StringType, true)))

    val orderDf = spark.read
      .option("sep",",")
      .schema(orderschema)
      .csv("s3a://paresh-first-aws-s3-bucket/data-master/retail_db/orders")


    val customerschema = StructType(Array(StructField("customer_id", IntegerType, true),
      StructField("customer_fname", StringType, true),
      StructField("customer_lname", StringType, true))
    )

    val customerDf = spark.read
      .option("sep",",")
      .schema(customerschema)
      .csv("s3a://paresh-first-aws-s3-bucket/data-master/retail_db/customers")


    val distinctCustOrder = orderDf.select("order_customer_id")
      .distinct()
      .withColumnRenamed("order_customer_id","customer_id")

    val invalidCustDF = customerDf
      .join(distinctCustOrder,Seq("customer_id"),"leftanti")
    //write to s3 bucket
    //invalidCustDF.write.save("s3a://paresh-first-aws-s3-bucket/output/s3sparkdemo")
    invalidCustDF.show()
  }

}
