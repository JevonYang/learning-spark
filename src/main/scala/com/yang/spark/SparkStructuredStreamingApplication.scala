package com.yang.spark

import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, LocalTime}

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql._
import org.apache.spark.sql.streaming.OutputMode


object SparkStructuredStreamingApplication {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("spark-structured-streaming-application")
      .getOrCreate()

    import spark.implicits._

    // 对于读取数据流的一些配置
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "zipkin")
      .option("kafka.sasl.mechanism", "PLAIN")
      .option("kafka.security.protocol", "SASL_PLAINTEXT")
      .option("enable.auto.commit", false) // Cannot be set to true in Spark Strucutured Streaming https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html#kafka-specific-configurations
      .option("failOnDataLoss", false)
      .option("startingOffsets", "earliest")
      .load()

    // 非case class 或者非product子类的 需要一个encoder，格式大概如此，但是下面的内容无用
    //        import org.apache.spark.sql.Encoders
    //        implicit val SpansEncoder = Encoders.javaSerialization[java.util.List[Span]]
    //        implicit val SpanEncoder = Encoders.javaSerialization[Span]

    // 将字段进行映射
    val endPoint = new StructType()
      .add("serviceName", DataTypes.StringType)
      .add("ipv4", DataTypes.StringType)
      .add("ipv4Bytes", DataTypes.BinaryType)
      .add("ipv6", DataTypes.StringType)
      .add("ipv6Bytes", DataTypes.BinaryType)
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
      .select("span.traceId", "span.parentId", "span.id", "span.kind", "span.name", "span.timestamp", "span.duration", "span.tags", "span.localEndpoint", "span.remoteEndpoint").as[Span]


    val processedDf = jsonDataframe
      .filter(span => span.name != "post" && span.name != "get" && span.name != null)
//      .filter(span => span.tags.get("http.method") == "POST")
//      .filter(span => span.localEndpoint.serviceName=="mip-authtenantserver")

    // 利用udf对字段进行处理
//    val processedDf = jsonDataframe
//      .where(col("name").=!=("post"))
//      .where(col("name").=!=("get"))
//      .where(col("name").=!=("null"))
//      .where(col("timestamp").>=(DateTimeUtils.oneDayBeforeInTimestamp()*1000))
//      .where(col("tags").getItem("http.method").===("POST"))
//      .where(col("localEndpoint").cast(endPoint).getField("serviceName").===("mip-authtenantserver"))
//      .withColumn("time", timestampUdf.apply($"timestamp"))
//      .groupBy("time").count()
//      .sort($"count".desc)

    // 展示或者存储
    val result = processedDf
      .writeStream
      .outputMode(OutputMode.Append())
//      .option("checkpointLocation", "./check")
      .format("console")
      .start()

    result.awaitTermination()
  }

}
