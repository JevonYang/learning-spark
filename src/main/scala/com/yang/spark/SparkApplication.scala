package com.yang.spark

import org.apache.spark.{SparkConf, SparkContext}

object SparkApplication {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("first-spark-application").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val distFile = sc.textFile("data.txt")
    val lineLength = distFile.map(s => s.length)
    val totalLength = lineLength.reduce((a, b) => a+b)
    println(totalLength)

    val broadcastVar = sc.broadcast(Array(1,2,3))
    broadcastVar.value.foreach(println)

  }

}
