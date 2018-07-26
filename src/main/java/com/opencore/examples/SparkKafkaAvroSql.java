package com.opencore.examples;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkKafkaAvroSql {

    public static void main(String[] args) {

        //configure Spark
        SparkConf conf = new SparkConf()
                .setAppName("kafka-structured")
                .setMaster("local[*]");

        SparkSession spark = SparkSession
                .builder()
                .config(conf)
                .appName("JavaStructuredNetworkWordCount")
                .getOrCreate();

        Dataset<Row> ds1 = spark
                                .readStream()
                                .format("kafka")
                                .option("kafka.bootstrap.servers", "localhost:9092")
                                .option("subscribe", "topic1")
                                .load();







    }

}
