name := "structured-streaming-cassandra"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "2.2.1",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.2.1",
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.2"
)