package com.knoldus.services

import com.knoldus.helper.CassandraSink
import com.knoldus.models.Employee
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}

object KafkaToCassandraApplication extends App {

  val spark = SparkSession
    .builder()
    .appName("Kafka_To_Cassandra")
    .master("local[*]")
    .config("spark.cassandra.connection.host", "localhost")
    .config("spark.cassandra.connection.port", "9042")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  val df = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers","localhost:9092")
    .option("startingOffsets","earliest")
    .option("subscribe","employee")
    .load()

  val parsedDF = df.select(col("value"))
    .select(col("value").cast("STRING"))
    .select(from_json(col("value"), Employee.schema).as("data"))
    .select("data.*")

    parsedDF
    .writeStream
    .outputMode("update")
    .option("checkpointLocation","src/main/resources/checkpoints")
    .foreach(new CassandraSink(spark.sparkContext.getConf))
    .start()
    .awaitTermination()

}
