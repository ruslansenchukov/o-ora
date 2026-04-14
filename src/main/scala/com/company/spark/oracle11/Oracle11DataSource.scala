package com.company.spark.oracle11

import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class Oracle11DataSource extends TableProvider with DataSourceRegister {

  override def shortName(): String = "oracle11"

  override def inferSchema(options: CaseInsensitiveStringMap): StructType =
    Oracle11SchemaInference.infer(Oracle11Options.fromCaseInsensitive(options))

  override def supportsExternalMetadata(): Boolean = true

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: java.util.Map[String, String]): Table = {

    val options = Oracle11Options.fromJava(properties)
    val resolvedSchema = Option(schema).filterNot(_.isEmpty).getOrElse(Oracle11SchemaInference.infer(options))
    new Oracle11Table(options, resolvedSchema)
  }
}
