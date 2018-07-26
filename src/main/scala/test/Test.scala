package test

import io.confluent.kafka.schemaregistry.client.rest.RestService
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.Schema
import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp

import collection.JavaConverters._



object Test {

  case class KafkaMessage(key: Array[Byte], value: Array[Byte],topic: String, partition: String, offset: Long, timestamp: Timestamp)

  def main(args: Array[String]): Unit = {

    val schemaRegistryURL = "http://127.0.0.1:8081"
    val topicName = "sea_vessel_position_reports"
    val subjectValueName = topicName + "-value"

    //create RestService object

    val restService = new RestService(schemaRegistryURL)
    //.getLatestVersion returns io.confluent.kafka.schemaregistry.client.rest.entities.Schema object.
    val valueRestResponseSchema = restService.getLatestVersion(subjectValueName)


    //Use Avro parsing classes to get Avro Schema
    val parser = new Schema.Parser
    val topicValueAvroSchema: Schema = parser.parse(valueRestResponseSchema.getSchema)
    println(topicValueAvroSchema.toString(true))

    //key schema is typically just string but you can do the same process for the key as the value
    val keySchemaString = "\"string\""
    val keySchema = parser.parse(keySchemaString)

    //Create a map with the Schema registry url.
    //This is the only Required configuration for Confluent's KafkaAvroDeserializer.
    val props = Map("schema.registry.url" -> schemaRegistryURL)

    //Declare SerDe vars before using Spark structured streaming map. Avoids non serializable class exception.
    var keyDeserializer: KafkaAvroDeserializer = null
    var valueDeserializer: KafkaAvroDeserializer = null

    val sql = SparkSession.builder().master("local").getOrCreate()

    import sql.implicits._

    //Create structured streaming DF to read from the topic.
    val rawTopicMessageDF = sql.readStream
                               .format("kafka")
                               .option("kafka.bootstrap.servers", "localhost:9092")
                               .option("subscribe", topicName)
                               .option("startingOffsets", "earliest")
                               //.option("maxOffsetsPerTrigger", 20)  //remove for prod
                               .load()
                               .as[KafkaMessage]

      ///.select("key","value")


    rawTopicMessageDF.printSchema()

  //  rawTopicMessageDF.show(5)

   var console = rawTopicMessageDF.writeStream
                     .outputMode("append")
                     .format("console")
                     .option("truncate", false)
                     .start()

    console.awaitTermination()





    //instantiate the SerDe classes if not already, then deserialize!
  /*  val deserializedTopicMessageDS = rawTopicMessageDF.map {
      row =>

        if (keyDeserializer == null) {
          keyDeserializer = new KafkaAvroDeserializer
          keyDeserializer.configure(props.asJava, true) //isKey = true
        }

        if (valueDeserializer == null) {
          valueDeserializer = new KafkaAvroDeserializer
          valueDeserializer.configure(props.asJava, false) //isKey = false
        }

      //Pass the Avro schema.
      val deserializedKeyString = keyDeserializer.deserialize(topicName, row.key, keySchema).toString //topic name is actually unused in the source code, just required by the signature. Weird right?
      val deserializedValueJsonString = valueDeserializer.deserialize(topicName, row.value, topicValueAvroSchema).toString




    }*/

}

}
