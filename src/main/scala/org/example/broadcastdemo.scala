package org.example

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Logger, LogManager}

object broadcastdemo{

  def main(args: Array[String]): Unit = {
    val logger = LogManager.getLogger(getClass.getName)

    val spark = SparkSession.builder()
      .appName("BroadcastDemo")
      .master("local[*]")
      .getOrCreate()

    val states = Map(("NY","New York"),("CA","California"),("FL","Florida"))
    val countries = Map(("USA","United States of America"),("IN","India"))

    //define broadcast variables for state & countries
    val broadcastStates = spark.sparkContext.broadcast(states)
    val broadcastCountry = spark.sparkContext.broadcast(countries)

    val data = Seq(("James","Smith","USA","CA"),
      ("Michael","Rose","USA","NY"),
      ("Robert","Williams","USA","CA"),
      ("Maria","Jones","USA","FL")
    )

    val columns = Seq("firstname","lastname","country","state")
    import spark.sqlContext.implicits._
    val df = data.toDF(columns:_*)

    val df2 = df.map(row => {
      val country = row.getString(2)
      val state = row.getString(3)
      //get value from broadcasted df
      val fullCountry = broadcastCountry.value.get(country).get
      val fullState = broadcastStates.value.get(state).get
      (row.getString(0),row.getString(1),fullCountry,fullState)
    }).toDF(columns:_*)
    df2.show(false)
  }
}
