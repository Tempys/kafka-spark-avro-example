/*package testimport org.apache.sparkimport org.apache.spark.sql.SparkSessionimport za.co.absa.abris.avro.read.confluent.SchemaManagerimport za.co.absa.abris.avro.schemas.policy.SchemaRetentionPolicies.RETAIN_SELECTED_COLUMN_ONLYobject AbrisScalaTest {  def main(args: Array[String]): Unit = {    val schemaRegistryConfs = Map(      SchemaManager.PARAM_SCHEMA_REGISTRY_URL   -> "url_to_schema_registry",      SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC -> "topic_name",      SchemaManager.PARAM_SCHEMA_ID_LATEST_NAME            -> "earliest" // otherwise, just specify an id    )    // import Spark Avro Dataframes    import za.co.absa.abris.avro.AvroSerDe._    val spark = SparkSession.builder().master("local").getOrCreate()    val stream = spark      .readStream      .format("kafka")      .option("kafka.bootstrap.servers", "localhost:9092")      .option("subscribe", "test-topic")      .fromConfluentAvro("column_containing_avro_data", None, Some(schemaRegistryConfs))(RETAIN_SELECTED_COLUMN_ONLY) // invoke the library passing over parameters to access the Schema Registry    stream.filter("field_x % 2 == 0")      .writeStream.format("console").start().awaitTermination()  }}*/