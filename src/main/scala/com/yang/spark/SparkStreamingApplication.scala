package com.yang.spark

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{StreamingContext, _}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import zipkin2.Span
import zipkin2.codec.SpanBytesDecoder

object SparkStreamingApplication {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark-streaming-application").setMaster("local[*]")
    val streamingContext = new StreamingContext(conf, Seconds(5))

    // streamingContext.checkpoint("checkpoint") 10.124.210.54:9092,10.124.210.54:9093,

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092,localhost:9093,localhost:9092,localhost:9093",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[ByteArrayDeserializer],
      "sasl.mechanism" -> "PLAIN",
      "security.protocol" -> "SASL_PLAINTEXT",
      "group.id" -> "spark-zipkin",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("zipkin")
    val stream: InputDStream[ConsumerRecord[String, Array[Byte]]] = KafkaUtils.createDirectStream[String, Array[Byte]](
      streamingContext,
      PreferConsistent,
      Subscribe[String, Array[Byte]](topics, kafkaParams)
    )

    val spans: DStream[java.util.List[Span]] = stream.map { case cr => SpanBytesDecoder.JSON_V2.decodeList(cr.value()) }

    val count = spans.map(println).count()

    count.print()

    streamingContext.start()
    streamingContext.awaitTermination()
  }

}
