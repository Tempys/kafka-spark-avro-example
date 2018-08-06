package test

import dubovskyi.avro.read.confluent.SchemaManager
import dubovskyi.avro.schemas.policy.SchemaRetentionPolicies.RETAIN_SELECTED_COLUMN_ONLY
import dubovskyi.avro.serde.AvroDecoder
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import test.KafkaToMemSqlStream.createschemaRegistryConfs
import com.memsql.spark.connector._
import com.memsql.spark.connector.util._
import com.memsql.spark.connector.util.JDBCImplicits._



object KafkaAvroDataBricks {

  case class KafkaMessage(key: String, value: Array[Byte],topic: String, partition: String, offset: Long, timestamp: Timestamp)

  def main(args: Array[String]): Unit = {


    Logger.getLogger("org").setLevel(Level.DEBUG)
    val kafkaHost = "94.130.90.122"
    val schemaRegistryURL = s"http://$kafkaHost:8081"
    val topicNamefirst = "SessionDetails_Joined_test4"
    val subjectValueNamefirst = topicNamefirst + "-value"


    import io.confluent.kafka.schemaregistry.client.rest.RestService
    val restService = new RestService(schemaRegistryURL)
    val valueRestResponseSchema = restService.getLatestVersion(subjectValueNamefirst)


    println(valueRestResponseSchema.getId.toString)

    var conf = new SparkConf()
      .setAppName("Write to MemSQL Example")
      .set("spark.memsql.host", "192.168.99.100")
      .set("spark.memsql.port", "32771")
      .set("spark.memsql.user", "root")
      .set("spark.memsql.password", "")
      .set("spark.memsql.defaultDatabase", "test")
      .set("spark.memsql.defaultDatabase", "test")

    // import Spark Avro Dataframes

    val spark = SparkSession.builder().config(conf).master("local").getOrCreate()
    var  avroDecoder = new AvroDecoder()
    import spark.implicits._
    import  dubovskyi.avro.serde._
    import  dubovskyi.avro.AvroSerDe._

    val streamFromFirstTopic = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", s"$kafkaHost:9092")
      .option("startingOffsets", "earliest")
      .option("subscribe",topicNamefirst)
       .load()
     // .fromConfluentAvro("value", None, Some(createschemaRegistryConfs(schemaRegistryURL,topicNamefirst,valueRestResponseSchema)))(RETAIN_SELECTED_COLUMN_ONLY) // invoke the library passing over parameters to access the Schema Registry
      .as[KafkaMessage]
      .map(row => row.value)
     // .toDF()
      //.saveToMemSQL("test.test_table6")

    streamFromFirstTopic
                       .write
                       .format("console")
                       .save()


  /*  streamFromFirstTopic
                       .write
      .format("com.memsql.spark.connector")
      .mode("error")
      .option("insertBatchSize",100)
      .save("test.test2")*/

    //.select(from_avr
     // .map(i => avroDecoder.fromConfluentAvroToRow(i,valueRestResponseSchema,createschemaRegistryConfs(schemaRegistryURL,topicNamefirst,valueRestResponseSchema))
     /* */
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
