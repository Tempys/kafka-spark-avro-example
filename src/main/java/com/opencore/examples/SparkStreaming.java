package com.opencore.examples;

import java.util.*;

import io.confluent.kafka.serializers.KafkaAvroDecoder;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import scala.Tuple2;

public class SparkStreaming {

  public static void main(String... args) {

    SparkConf conf = new SparkConf();
    conf.setMaster("local[2]");
    conf.setAppName("Spark Streaming test.Test Java");

    JavaSparkContext sc = new JavaSparkContext(conf);
   // sc.setLogLevel("DEBUG");
    JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(10));

    Map<String, Object> kafkaParams = new HashMap<>();
    kafkaParams.put("bootstrap.servers", "localhost:9092");
    kafkaParams.put("key.deserializer", StringDeserializer.class);
    kafkaParams.put("value.deserializer", StringDeserializer.class);
    kafkaParams.put("group.id", "use_a_separate_group_id_for_eacsh_sstream");
    kafkaParams.put("auto.offset.reset", "earliest");

    Collection<String> topics = Arrays.asList("backblaze_smart");

    JavaInputDStream<ConsumerRecord<String, String>> stream =
            KafkaUtils.createDirectStream(
                    ssc,
                    LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
            );


    stream.foreachRDD(rdd -> {

      OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();

    /*  for (OffsetRange o : offsetRanges){
      System.out.println(o.  " ${o.partition} offsets: ${o.fromOffset} to ${o.untilOffset}")
      }*/


    });


  }


}

