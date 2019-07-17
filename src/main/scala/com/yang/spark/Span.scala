package com.yang.spark

case class Span(traceId: String, parentId: String, id: String, kind: String, name: String, timestamp: Option[Long], tags: Map[String, String], localEndpoint: EndPoints, remoteEndpoint: EndPoints)

case class EndPoints(serviceName: String, ipv4: String, ipv4Bytes: Option[String], ipv6: String, ipv6Bytes: Option[String], port: String)
