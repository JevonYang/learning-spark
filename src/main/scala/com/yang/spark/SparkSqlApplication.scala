package com.yang.spark

import java.time.{LocalDateTime, LocalTime}
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{DataTypes, StructType}

object SparkSqlApplication {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("spark-sql-application")
      .getOrCreate()

    import spark.implicits._

    // 对于读取数据流的一些配置
    val df = spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "zipkin")
      .option("kafka.sasl.mechanism", "PLAIN")
      .option("kafka.security.protocol", "SASL_PLAINTEXT")
      .option("enable.auto.commit", false) // Cannot be set to true in Spark Strucutured Streaming https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html#kafka-specific-configurations
      .option("failOnDataLoss", false)
      .option("startingOffsets", "earliest")
      .load()

    // 将字段进行映射
    val endPoint = new StructType()
      .add("serviceName", DataTypes.StringType)
      .add("ipv4", DataTypes.StringType)
      .add("ipv4Bytes", DataTypes.ByteType)
      .add("ipv6", DataTypes.StringType)
      .add("ipv6Bytes", DataTypes.ByteType)
      .add("port", DataTypes.StringType)

    val schema = new StructType()
      .add("traceId", DataTypes.StringType)
      .add("parentId", DataTypes.StringType)
      .add("id", DataTypes.StringType)
      .add("kind", DataTypes.StringType)
      .add("name", DataTypes.StringType)
      .add("timestamp", DataTypes.LongType)
      .add("duration", DataTypes.LongType)
      .add("tags", DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType))
      .add("localEndpoint", endPoint)
      .add("remoteEndpoint", endPoint)

    // 自定义函数
    val timestampFunc: Long => String = timestamp => {
      val time = DateTimeUtils.getDateTimeOfTimestamp(timestamp / 1000)
      LocalDateTime.of(time.toLocalDate, LocalTime.of(time.getHour, 0)).format(DateTimeFormatter.ISO_DATE_TIME)
    }

    val timestampUdf: UserDefinedFunction = functions.udf(timestampFunc, DataTypes.StringType)

    // 引入 from_json col等函数
    import org.apache.spark.sql.functions._

    // 映射各个字段
    val jsonDataframe = df.selectExpr("CAST(value AS STRING)")
      .select(from_json(col("value"), schema).as("span"))
      .select("span.traceId", "span.parentId", "span.id", "span.kind", "span.name", "span.timestamp", "span.duration", "span.tags", "span.localEndpoint", "span.remoteEndpoint")

    jsonDataframe
      .where(col("timestamp").>=(DateTimeUtils.oneDayBeforeInTimestamp()*1000))
      .where(col("kind").=!=("CLIENT"))
      .where(col("name").=!=("get"))
//      .filter(span => span.timestamp.getOrElse(0) =<  (DateTimeUtils.oneDayBeforeInTimestamp()*1000))
//      .filter(span => span.kind == "SERVER")
      .withColumn("time", timestampUdf.apply($"timestamp"))
      .groupBy($"name", $"time")
      .count()
//      .agg(mean($"duration"))
//      .agg(count("traceId"))
//      .select($"localEndpoint", $"tags")
      .show()
  }

}
