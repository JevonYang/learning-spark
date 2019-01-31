package com.yang.spark

import org.apache.spark.sql.SparkSession


object SparkSQLApplication {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("spark-sql-application")
      .getOrCreate()

    import spark.implicits._

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092,localhost:9093,localhost:9092,localhost:9093")
      .option("subscribe", "gateway-log")
      .load()

    val query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]

    val result = query
        .writeStream
      .outputMode("complete")
      .option("checkpointLocation", "./check")
      .format("console")
//        .format("json")
//        .option("checkpointLocation", "./check")
//        .option("path", "./log")
        .start()
    result.awaitTermination()
  }

}
