package com.yang.spark

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, LocalTime}

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql._


object SparkSQLApplication {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("spark-sql-application")
      .getOrCreate()

    import spark.implicits._

    // 对于读取数据流的一些配置
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092,localhost:9093,localhost:9092,localhost:9093")
      .option("subscribe", "gateway-log")
      .load()

    // 非case class 或者非product子类的 需要一个encoder，格式大概如此，但是下面的内容无用
    //        import org.apache.spark.sql.Encoders
    //        implicit val SpansEncoder = Encoders.javaSerialization[java.util.List[Span]]
    //        implicit val SpanEncoder = Encoders.javaSerialization[Span]

    // 将字段进行映射
    val endPoint = new StructType()
      .add("serviceName", DataTypes.StringType)
      .add("ipv4", DataTypes.StringType)
      .add("ipv4Bytes", DataTypes.ByteType)
      .add("ipv6", DataTypes.StringType)
      .add("ipv6Bytes", DataTypes.ByteType)
      .add("port", DataTypes.StringType)

    val schema = StructType(Array(
      StructField("traceId", DataTypes.StringType),
      StructField("parentId", DataTypes.StringType),
      StructField("id", DataTypes.StringType),
      StructField("kind", DataTypes.StringType),
      StructField("name", DataTypes.StringType),
      StructField("timestamp", DataTypes.LongType),
      StructField("duration", DataTypes.LongType),
      StructField("tags", DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType)),
      StructField("localEndpoint", endPoint),
      StructField("remoteEndpoint", endPoint),
    ))


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

    // 利用udf对字段进行处理
    val processedDf = jsonDataframe.withColumn("time", timestampUdf.apply($"timestamp"))

    // 展示或者存储
    val result = processedDf
      .writeStream
      .outputMode("append")
      .option("checkpointLocation", "hdfs://localhost:9000/check")
      .format("console")
      .start()

    result.awaitTermination()
  }

}
