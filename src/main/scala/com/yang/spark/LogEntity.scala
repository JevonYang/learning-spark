package com.yang.spark

case class LogEntity(remoteAddress: String,
                     method: String,
                     path: String,
                     param: Map[String, Array[String]],
                     headers: Map[String, Array[String]],
                     cookie: Map[String, Array[Map[String, String]]],
                     body: String,
                     time: Long)
