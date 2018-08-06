package test

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import test.KafkaSqlJoins.createschemaRegistryConfs
import za.co.absa.abris.avro.schemas.policy.SchemaRetentionPolicies.RETAIN_SELECTED_COLUMN_ONLY
import com.memsql.spark.connector._
import com.memsql.spark.connector.util._
import com.memsql.spark.connector.util.JDBCImplicits._
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema
import za.co.absa.abris.avro.read.confluent.SchemaManager

object KafkaToMemSqlStream {

  def main(args: Array[String]): Unit = {
 ///   Logger.getLogger("org").setLevel(Level.DEBUG)

    val schemaRegistryURL = "http://94.130.90.122:8081"
    val topicNamefirst = "ErrorReport_Joined_test1"
    val subjectValueNamefirst = topicNamefirst + "-value"


    import io.confluent.kafka.schemaregistry.client.rest.RestService
    val restService = new RestService(schemaRegistryURL)
    val valueRestResponseSchema = restService.getLatestVersion(subjectValueNamefirst)


    println(valueRestResponseSchema.getId.toString)

    println("=============== start =======================")

    var conf = new SparkConf()


      .setAppName("Write to MemSQL Example")
      .set("spark.memsql.host", "192.168.99.100")
      .set("spark.memsql.port", "32771")
      .set("spark.memsql.user", "root")
      .set("spark.memsql.password", "")
      .set("spark.memsql.defaultDatabase", "kafka_sql")



    // import Spark Avro Dataframes
    import za.co.absa.abris.avro.AvroSerDe._

    val spark = SparkSession.builder()
                            .config(conf)
                            .master("local").getOrCreate()

    val streamFromFirstTopic = spark
                                    .readStream
                                    .format("kafka")
                                    .option("kafka.bootstrap.servers", "94.130.90.122:9092")
                                    .option("startingOffsets", "earliest")
                                    .option("subscribe",topicNamefirst)
                                    .option("maxOffsetsPerTrigger",1000)
                                     .load()
                                    .fromConfluentAvro("value", None, Some(createschemaRegistryConfs(schemaRegistryURL,topicNamefirst,valueRestResponseSchema)))(RETAIN_SELECTED_COLUMN_ONLY) // invoke the library passing over parameters to access the Schema Registry

      //.load()

                                   // .toDF()
                                  //  .saveToMemSQL("test.test_table2")

    streamFromFirstTopic
                      .write
                      .format("console")
      .save()

     /* .writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", "false")
      .start()
      .awaitTermination()*/

       // Block execution, to force Zeppelin to capture the output
                                   // .fromConfluentAvro("value", None, Some(createschemaRegistryConfs(schemaRegistryURL,topicNamefirst,valueRestResponseSchema)))(RETAIN_SELECTED_COLUMN_ONLY) // invoke the library passing over parameters to access the Schema Registry
                                   // .toDF()
     // .count()
                                    //.saveToMemSQL("kafka_sql.spark_error_report")
     //print("result"+streamFromFirstTopic.count())
    /*   streamFromFirstTopic
         .write
         .format("console")
         .save()
   */


  /*  streamFromFirstTopic
                        .write
                        .format("com.memsql.spark.connector")
                        .option("insertBatchSize",100)
                       //.mode("ignore")
                        .save("kafka_sql.spark_test2")*/








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
