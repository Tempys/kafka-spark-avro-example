package test

import org.apache.spark.sql.SparkSession
import za.co.absa.abris.avro.read.confluent.SchemaManager
import za.co.absa.abris.avro.schemas.policy.SchemaRetentionPolicies.RETAIN_SELECTED_COLUMN_ONLY

object Test1 {

  def main(args: Array[String]): Unit = {




    val schemaRegistryConfs = Map(
      SchemaManager.PARAM_SCHEMA_REGISTRY_URL   -> "http://localhost:8081",
      SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC -> "sea_vessel_position_reports",
      SchemaManager.PARAM_VALUE_SCHEMA_ID -> "3"
    )

    // import Spark Avro Dataframes
    import za.co.absa.abris.avro.AvroSerDe._
    val spark = SparkSession.builder().master("local").getOrCreate()

    val stream = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("startingOffsets", "earliest")
      .option("subscribe", "sea_vessel_position_reports")
      .fromConfluentAvro("value", None, Some(schemaRegistryConfs))(RETAIN_SELECTED_COLUMN_ONLY) // invoke the library passing over parameters to access the Schema Registry

    stream
      .writeStream.format("console").start().awaitTermination()


  }
}
