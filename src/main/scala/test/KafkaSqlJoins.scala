package test

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import za.co.absa.abris.avro.read.confluent.SchemaManager
import za.co.absa.abris.avro.schemas.policy.SchemaRetentionPolicies.RETAIN_SELECTED_COLUMN_ONLY

object KafkaSqlJoins {

  def main(args: Array[String]): Unit = {

    val topicName = "sea_vessel_position_reports"
    val schemaRegistryURL = "http://127.0.0.1:8081"
    val subjectValueName = topicName + "-value"

    import io.confluent.kafka.schemaregistry.client.rest.RestService
    val restService = new RestService(schemaRegistryURL)
    val valueRestResponseSchema = restService.getLatestVersion(subjectValueName)


    println(valueRestResponseSchema.getId.toString)

    val schemaRegistryConfs = Map(
      SchemaManager.PARAM_SCHEMA_REGISTRY_URL   -> schemaRegistryURL,
      SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC -> topicName,
      SchemaManager.PARAM_VALUE_SCHEMA_ID -> valueRestResponseSchema.getId.toString
    )


    var conf = new SparkConf()
      .setAppName("Write to MemSQL Example")
      .set("spark.memsql.host", "localhost")
      .set("spark.memsql.port", "3306")
      .set("spark.memsql.user", "root")
      .set("spark.memsql.password", "pass")
      .set("spark.memsql.defaultDatabase", "test")


    // import Spark Avro Dataframes
    import za.co.absa.abris.avro.AvroSerDe._
    val spark = SparkSession.builder().master("local").getOrCreate()

    val stream = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("startingOffsets", "earliest")
      .option("subscribe", "sea_vessel_position_reports")
      .load()
      .fromConfluentAvro("value", None, Some(schemaRegistryConfs))(RETAIN_SELECTED_COLUMN_ONLY) // invoke the library passing over parameters to access the Schema Registry
  /*    .write
      .format("com.memsql.spark.connector")
      .mode("error")
      .save("people.students")*/

        stream
          .write
          .format("console")
          .save()

  }

}
