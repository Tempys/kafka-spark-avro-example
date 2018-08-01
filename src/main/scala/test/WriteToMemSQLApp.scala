package test

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.SparkConf
import com.memsql.spark.connector._
import com.memsql.spark.connector.util._
import com.memsql.spark.connector.util.JDBCImplicits._

object WriteToMemSQLApp {
  def main(args: Array[String]) {
    val connInfo = MemSQLConnectionInfo("127.0.0.1", 3306, "root", "", "information_schema")

    var conf = new SparkConf()
      .setAppName("Write to MemSQL Example")
      .set("spark.memsql.host", "94.130.90.122")
      .set("spark.memsql.port", "3306")
      .set("spark.memsql.user", "root")
      .set("spark.memsql.password", "eNt#E2tzeNt#E2tz")
      .set("spark.memsql.defaultDatabase", "kafka_sql")

  //  val ss = SparkSession.builder().config(conf).master("local").getOrCreate()
    val ss = SparkSession.builder().master("local").config(conf).getOrCreate()
    import ss.implicits._

    val rdd =  ss.sparkContext.parallelize(Array(Row("John Smith", 12), Row("Jane Doe", 13)))
    val schema = StructType(Seq(StructField("Name", StringType, false),
      StructField("Age", IntegerType, false)))
    val df = ss.createDataFrame(rdd, schema)
    df.saveToMemSQL("kafka_sql.students")
  }
}