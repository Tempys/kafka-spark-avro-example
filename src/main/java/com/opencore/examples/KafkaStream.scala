package com.opencore.examples

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}

class KafkaStream {


  def main(args: Array[String]) {
    val preferredHosts = LocationStrategies.PreferConsistent
    val topics = List("coyote-test-json")

    val kafkaParams = Map(
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "spark-streaming-notes",
      "auto.offset.reset" -> "earliest"
    )


    val offsets = Map(new TopicPartition("topic3", 0) -> 2L)
    val sc = SparkContext.getOrCreate
    val ssc = new StreamingContext(sc, Seconds(5))
    val dstream = KafkaUtils.createDirectStream[String, String](
      ssc,
      preferredHosts,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams, offsets))

    dstream.foreachRDD { rdd =>
      // Get the offset ranges in the RDD
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      for (o <- offsetRanges) {
        println(s"${o.topic} ${o.partition} offsets: ${o.fromOffset} to ${o.untilOffset}")
      }
    }

    ssc.start

    // the above code is printing out topic details every 5 seconds
    // until you stop it.

    ssc.stop(stopSparkContext = false)
  }

}
