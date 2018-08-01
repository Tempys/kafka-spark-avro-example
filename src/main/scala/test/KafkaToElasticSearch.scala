package test

import org.apache.log4j.Level
import org.apache.log4j.Logger

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import test.KafkaSqlJoins.createschemaRegistryConfs
import za.co.absa.abris.avro.schemas.policy.SchemaRetentionPolicies.RETAIN_SELECTED_COLUMN_ONLY

object KafkaToElasticSearch {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.DEBUG)

    val schemaRegistryURL = "http://192.168.99.100:8081"
    val topicNamefirst = "sea_vessel_position_reports"
    val subjectValueNamefirst = topicNamefirst + "-value"


    import io.confluent.kafka.schemaregistry.client.rest.RestService
    val restService = new RestService(schemaRegistryURL)
    val valueRestResponseSchema = restService.getLatestVersion(subjectValueNamefirst)


    println(valueRestResponseSchema.getId.toString)

    println("=============== start =======================")

    var conf = new SparkConf()
    conf.set("es.index.auto.create", "true")
    conf.set("es.nodes","192.168.99.100:9200")
    conf.set("es.nodes.wan.only","true")

    /* .setAppName("Write to MemSQL Example")
     .set("spark.memsql.host", "192.168.99.100")
     .set("spark.memsql.port", "3306")
     .set("spark.memsql.user", "root")
     .set("spark.memsql.password", "")
     .set("spark.memsql.defaultDatabase", "kafka_sql")*/



    // import Spark Avro Dataframes
    import za.co.absa.abris.avro.AvroSerDe._
    import org.elasticsearch.spark.sql._

    val spark = SparkSession.builder()
      .config(conf)
      .master("local").getOrCreate()




    val streamFromFirstTopic = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.99.100:9092")
      .option("startingOffsets", "earliest")
      .option("subscribe",topicNamefirst)
      .load()
      .fromConfluentAvro("value", None, Some(createschemaRegistryConfs(schemaRegistryURL,topicNamefirst,valueRestResponseSchema)))(RETAIN_SELECTED_COLUMN_ONLY) // invoke the library passing over parameters to access the Schema Registry
      .toDF()
      .saveToEs("spark/people")

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

}


