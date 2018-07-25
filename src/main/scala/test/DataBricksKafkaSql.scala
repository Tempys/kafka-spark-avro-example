package test

import org.apache.spark.sql.SparkSession

object DataBricksKafkaSql {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").getOrCreate()

    val ds1 = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "backblaze_smart")
      .load()


  /*  ds1.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
*/
  }


}
