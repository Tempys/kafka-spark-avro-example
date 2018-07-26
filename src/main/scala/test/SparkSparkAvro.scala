package test

import io.confluent.kafka.serializers.KafkaAvroDeserializer
import javassist.bytecode.stackmap.TypeTag
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.Decoder
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp

import scala.reflect.internal.util.Collections


object SparkSparkAvro {

  def main(args: Array[String]): Unit = {





    val spark = SparkSession.builder().master("local").getOrCreate()

    // import Spark Avro Dataframes


  }

}



/*object KafkaAvroConsumer {

  case class KafkaMessage(key: String, value: Array[Byte],topic: String, partition: String, offset: Long, timestamp: Timestamp)

  private val conf: Config = ConfigFactory.load().getConfig("kafka.consumer")
  val valueDeserializer = new KafkaAvroDeserializer()
  valueDeserializer.configure(Collections.singletonMap("schema.registry.url",  conf.getString("schema.registry.url")), false)

  def transform[T <: GenericRecord : TypeTag](msg: KafkaMessage, schemaStr: String) = {
    val schema = new Schema.Parser().parse(schemaStr)
    Utils.convert[T](schema)(valueDeserializer.deserialize(msg.topic, msg.value))
  }

  def createDataStream[T <: GenericRecord with Product with Serializable : TypeTag]
  (schemaStr: String)
  (subscribeType: String, topics: String, appName: String, startingOffsets: String = "latest") = {

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName(appName)
      .getOrCreate()

    import spark.implicits._

    // Create DataSet representing the stream of KafkaMessage from kafka
    val ds = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", conf.getString("bootstrap.servers"))
      .option(subscribeType, topics)
      .option("startingOffsets", "earliest")
      .load()
      .as[KafkaMessage]
      .map(msg => KafkaAvroConsumer.transform[T](msg, schemaStr)) // Transform it Avro object.

    ds
  }*/



