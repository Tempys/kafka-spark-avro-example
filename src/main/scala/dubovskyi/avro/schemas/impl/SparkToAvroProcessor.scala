package dubovskyi.avro.schemas.impl

import dubovskyi.avro.schemas.SchemasProcessor
import org.apache.spark.sql.types.StructType
import org.apache.avro.Schema
import dubovskyi.avro.format.SparkAvroConversions

/**
 * This class can produce Avro schemas from a Spark schema.
 */
class SparkToAvroProcessor(schema: StructType, schemaName: String, schemaNamespace: String) extends SchemasProcessor {
  
  def getAvroSchema(): Schema = {
    SparkAvroConversions.toAvroSchema(schema, schemaName, schemaNamespace)
  }
  
  def getSparkSchema(): StructType = {
    schema
  }
}