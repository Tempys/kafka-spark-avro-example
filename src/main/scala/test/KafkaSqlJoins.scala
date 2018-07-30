package test

import io.confluent.kafka.schemaregistry.client.rest.entities.Schema
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import za.co.absa.abris.avro.read.confluent.SchemaManager
import za.co.absa.abris.avro.schemas.policy.SchemaRetentionPolicies.RETAIN_SELECTED_COLUMN_ONLY

object KafkaSqlJoins {

  def main(args: Array[String]): Unit = {


    val schemaRegistryURL = "http://127.0.0.1:8081"
    val topicNamefirst = "telecom_italia_grid"
    val subjectValueNamefirst = topicNamefirst + "-value"
    val topicNamesecond = "telecom_italia_data"
    val subjectValueNamesecond = topicNamesecond + "-value"

    import io.confluent.kafka.schemaregistry.client.rest.RestService
    val restService = new RestService(schemaRegistryURL)
    val valueRestResponseSchema = restService.getLatestVersion(subjectValueNamefirst)
    val valueRestResponseSchemaSecond = restService.getLatestVersion(subjectValueNamesecond)


    println("second"+valueRestResponseSchemaSecond.getId.toString)
    println(valueRestResponseSchema.getId.toString)

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

    val streamFromFirstTopic = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("startingOffsets", "earliest")
      .option("subscribe",topicNamefirst)
      .load()
      .fromConfluentAvro("value", None, Some(createschemaRegistryConfs(schemaRegistryURL,topicNamefirst,valueRestResponseSchema)))(RETAIN_SELECTED_COLUMN_ONLY) // invoke the library passing over parameters to access the Schema Registry
  /*    .write
      .format("com.memsql.spark.connector")
      .mode("error")
      .save("people.students")*/


    val streamFromSecondTopic = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("startingOffsets", "earliest")
      .option("subscribe", topicNamesecond)
      .load()
      .fromConfluentAvro("value", None, Some(createschemaRegistryConfs(schemaRegistryURL,topicNamesecond,valueRestResponseSchemaSecond)))(RETAIN_SELECTED_COLUMN_ONLY) // invoke the library passing over parameters to access the Schema Registry


 var joinedStream =   streamFromFirstTopic.join(streamFromSecondTopic,streamFromFirstTopic.col("SquareId").startsWith(streamFromSecondTopic.col("SquareId")))

      streamFromFirstTopic
          .write
          .format("console")
          .save()

    streamFromSecondTopic
      .write
      .format("console")
      .save()




    joinedStream.write
                 .format("console")
                 .save()







  }


  def createschemaRegistryConfs(schemaRegistryURL :String,topic: String, schema: Schema):Map[String,String] ={

    val schemaRegistryConfs = Map(
      SchemaManager.PARAM_SCHEMA_REGISTRY_URL   -> schemaRegistryURL,
      SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC -> topic,
      SchemaManager.PARAM_VALUE_SCHEMA_ID -> schema.getId.toString

    )

    schemaRegistryConfs
  }



}
