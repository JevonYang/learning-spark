name := "learning-spark"

version := "0.1"

scalaVersion := "2.12.8"

libraryDependencies += "org.apache.spark" % "spark-core_2.12" % "2.4.0"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.12" % "2.4.0"

libraryDependencies += "org.apache.spark" % "spark-sql_2.12" % "2.4.0"

libraryDependencies += "com.fasterxml.jackson.core" % "jackson-core" % "2.9.8"

libraryDependencies += "org.apache.spark" % "spark-sql-kafka-0-10_2.12" % "2.4.0"

libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-10_2.12" % "2.4.0"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.9.2"

//<!-- https://mvnrepository.com/artifact/com.fasterxml.jackson.core/jackson-core -->
//  <dependency>
//    <groupId>com.fasterxml.jackson.core</groupId>
//    <artifactId>jackson-core</artifactId>
//    <version>2.9.8</version>
//  </dependency>
