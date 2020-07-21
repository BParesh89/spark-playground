package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import com.typesafe.config.{Config, ConfigFactory}
import java.io.File

object s3sparkdemo {
  def main(args: Array[String]): Unit = {

    //create spark session
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("invalidCustomer")
      .getOrCreate()
    //get aws access id and secret from conf file
    val config = ConfigFactory.parseFile(new File("src/main/resources/app.conf"))
      .getConfig("src.app.conf")
    //get aws configs
    val awsConfig = config.getConfig("aws")
    //get aws access key and secret key
    val s3_access_key = awsConfig.getString("access_key")
    val s3_secret_key = awsConfig.getString("secret_key")
    // Replace Key with your AWS account key (You can find this on IAM
    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.access.key", s3_access_key)
    // Replace Key with your AWS secret key (You can find this on IAM
    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.secret.key",s3_secret_key)
    //set endpoint to look for s3 file system
    spark.sparkContext
      .hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")

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
