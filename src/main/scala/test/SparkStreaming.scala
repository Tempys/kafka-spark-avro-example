package test

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.spark_project.guava.eventbus.Subscribe


/**
  * Created by afsalthaj on 14/05/2017.
  */
// Run a Landoop kafka docker container to have your kafka cluster up and running.
// and then run the spark streaming job
// the auto commit is false and configured to read from the `earliest`
object SparkStreaming {
  def main(args: Array[String]): Unit = {
    val kafkaParameters = Map[String, Object](
      "bootstrap.servers" -> "127.0.0.1:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "mygroupid",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)

    )

    val topics = Array("backblaze_smart")
    val conf = new SparkConf().setMaster("local")
      .setAppName("simply outputting the kafka messages in the producer to the console")
    // streaming will search for new offsets every 10 seconds from Kafka

    val ssc = new StreamingContext(conf, Seconds(10))
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParameters)
    )

    val keyValue: DStream[(String, String, String)] = stream.map(record =>
      (s"key is ${record.key}", s"""value is "${record.value}" """, s"partition is ${record.partition}"))

    keyValue.saveAsTextFiles("out/airports_in_usa.text")

    keyValue.print()
    ssc.start()
    ssc.awaitTermination()
  }
}