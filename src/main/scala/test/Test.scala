package test

import org.apache.spark.streaming.kafka010._
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf


object Test {


  def main(args: Array[String]): Unit = {

    import org.apache.spark.SparkContext
    import org.apache.spark.streaming._


    var conf = new SparkConf().setAppName("AirportsByLatitude").setMaster("local[2]")
    var context = new SparkContext(conf)

    val sc = SparkContext.getOrCreate
    val ssc = new StreamingContext(sc, Seconds(100))



    val preferredHosts = LocationStrategies.PreferConsistent
    val topics = List("backblaze_smart")
    import org.apache.kafka.common.serialization.StringDeserializer
    val kafkaParams = Map(
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "spark-streaming-notesff2",
      "auto.offset.reset" -> "earliest"
    )

    // val offsets = Map(new TopicPartition("backblaze_smart", 0) -> 2L)

    val dstream = KafkaUtils.createDirectStream[String, String](
      ssc,
      preferredHosts,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))

    /*dstream.foreachRDD { rdd =>
      // Get the offset ranges in the RDD
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      for (o <- offsetRanges) {
        println(s"${o.topic} ${o.partition} offsets: ${o.fromOffset} to ${o.untilOffset}")
      }
    }*/

    dstream.map(item => item.value()).foreachRDD(rrd => rrd.saveAsTextFile("out/test.txt"))

    ssc.start

    // the above code is printing out topic details every 5 seconds
    // until you stop it.

  //  ssc.stop(stopSparkContext = false)


  }

}
