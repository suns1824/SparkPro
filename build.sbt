name := "SparkPro"

version := "0.1"

scalaVersion := "2.11.8"

val hadoopVersion = "2.7.6"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.3.1"
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.3"
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.7.3"
libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.7.3"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.3.1"
libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.3.1"
libraryDependencies += "org.apache.hadoop" % "hadoop-yarn-server-web-proxy" % "2.7.3"
libraryDependencies += "org.apache.spark" % "spark-yarn_2.11" % "2.3.1"
libraryDependencies += "org.apache.spark" % "spark-repl_2.11" % "2.3.1"
libraryDependencies += "org.apache.spark" % "spark-streaming-flume_2.11" % "2.3.1"
libraryDependencies += "org.apache.spark" % "spark-streaming-flume-sink_2.11" % "2.3.1"
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.3.1"
libraryDependencies += "org.apache.flume.flume-ng-clients" % "flume-ng-log4jappender" % "1.6.0"
libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.2.0"
libraryDependencies += "org.apache.hbase" % "hbase-server" % "1.2.0"
libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.2.0"
libraryDependencies += "org.apache.hbase" % "hbase-protocol" % "1.2.0"

dependencyOverrides ++= Set(
  "io.netty" % "netty" % "3.9.9.Final",
  "commons-net" % "commons-net" % "3.1",
  "com.google.guava" % "guava" % "16.0.1",
  "com.google.code.findbugs" % "jsr305" % "3.0.2",
  "net.java.dev.jets3t" % "jets3t" % "0.9.4",
  "io.netty" % "netty-all" % "4.1.17.Final",
  "org.slf4j" % "slf4j-api" % "1.7.16",
  "xml-apis" % "xml-apis" % "1.3.04"
)
