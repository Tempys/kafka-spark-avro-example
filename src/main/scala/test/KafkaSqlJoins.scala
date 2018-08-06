package test

import io.confluent.kafka.schemaregistry.client.rest.entities.Schema
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import za.co.absa.abris.avro.read.confluent.SchemaManager
import za.co.absa.abris.avro.schemas.policy.SchemaRetentionPolicies.RETAIN_SELECTED_COLUMN_ONLY
import  org.apache.spark.sql.functions._
import com.memsql.spark.connector._
import com.memsql.spark.connector.util._
import com.memsql.spark.connector.util.JDBCImplicits._

object KafkaSqlJoins {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.DEBUG)
    val kafkaHost = "94.130.90.122"
    val schemaRegistryURL = s"http://$kafkaHost:8081"
    val topicNamefirst = "SessionDetails_Joined_test4"
    val subjectValueNamefirst = topicNamefirst + "-value"
    val topicNamesecond = "VoipDetails_Joined_test1"
    val subjectValueNamesecond = topicNamesecond + "-value"

    import io.confluent.kafka.schemaregistry.client.rest.RestService
    val restService = new RestService(schemaRegistryURL)
    val valueRestResponseSchema = restService.getLatestVersion(subjectValueNamefirst)
    val valueRestResponseSchemaSecond = restService.getLatestVersion(subjectValueNamesecond)


    println("second"+valueRestResponseSchemaSecond.getId.toString)
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
    import za.co.absa.abris.avro.AvroSerDe._
    val spark = SparkSession.builder().config(conf).master("local").getOrCreate()

    val streamFromFirstTopic = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", s"$kafkaHost:9092")
      .option("startingOffsets", "earliest")
      .option("subscribe",topicNamefirst)
      .load()
      .fromConfluentAvro("value", None, Some(createschemaRegistryConfs(schemaRegistryURL,topicNamefirst,valueRestResponseSchema)))(RETAIN_SELECTED_COLUMN_ONLY) // invoke the library passing over parameters to access the Schema Registry
      .withColumn("proccessingtime",lit(current_timestamp()))
      .withWatermark("proccessingtime","10 seconds")
    /*    .write
      .format("com.memsql.spark.connector")
      .mode("error")
      .save("people.students")*/


    val streamFromSecondTopic = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", s"$kafkaHost:9092")
      .option("startingOffsets", "earliest")
      //.option("")
      .option("subscribe", topicNamesecond)
      .load()
      .fromConfluentAvro("value", None, Some(createschemaRegistryConfs(schemaRegistryURL,topicNamesecond,valueRestResponseSchemaSecond)))(RETAIN_SELECTED_COLUMN_ONLY) // invoke the library passing over parameters to access the Schema Registry
      .withColumn("proccessingtimesecond",lit(current_timestamp()))
      .withWatermark("proccessingtimesecond","20 seconds")


    println("schema: "+ streamFromFirstTopic.schema)



    var joinedStream =   streamFromFirstTopic.alias("a").join(streamFromSecondTopic.alias("b"),
                                                                         streamFromFirstTopic.col("SessionIDSeq").<=>(streamFromSecondTopic.col("SessionIDSeq"))
                                                                                  // .&&(streamFromFirstTopic.col("sessionidtime").<=>(streamFromSecondTopic.col("sessionidtime"))
                                                                                  // .&&(streamFromFirstTopic.col("sessionidtime").>=(streamFromSecondTopic.col("sessionidtime")))
                                                                                   .&&(streamFromFirstTopic.col("proccessingtime").>=(streamFromSecondTopic.col("proccessingtimesecond")))
                                                                                   .&&(streamFromFirstTopic.col("proccessingtime").<=(streamFromSecondTopic.col("proccessingtimesecond")))
                                                                         )
      .select(streamFromFirstTopic.columns.map(streamFromFirstTopic(_)) : _*)
      .saveToMemSQL("test.test_table")

      /*streamFromFirstTopic
                          .write
                          .format("console")
                          .save()*/

   /*   streamFromSecondTopic
                         .write
                         .format("console")
                         .save() */

 /*  var result = joinedStream.write
                 .format("console")
                 .save()*/

  //  result.awaitTermination()

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
