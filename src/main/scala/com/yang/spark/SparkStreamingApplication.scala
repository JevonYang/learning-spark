package com.yang.spark

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{StreamingContext, _}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

object SparkStreamingApplication {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark-streaming-application").setMaster("local[*]")
    val streamingContext = new StreamingContext(conf, Seconds(1))

    // streamingContext.checkpoint("checkpoint")

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092,localhost:9093,localhost:9092,localhost:9093",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "gateway",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("gateway-log")
    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val lines = stream.map(item => item.value())
    val words = lines.flatMap(_.split("value"))

    words.print()

    streamingContext.start()
    streamingContext.awaitTermination()
  }

}
