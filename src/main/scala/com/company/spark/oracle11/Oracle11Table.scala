package com.company.spark.oracle11

import java.util

import scala.collection.JavaConverters._

import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

final class Oracle11Table(options: Oracle11Options, tableSchema: StructType)
    extends Table
    with SupportsRead
    with Serializable {

  override def name(): String = s"oracle11(${options.relation.description})"

  override def schema(): StructType = tableSchema

  override def capabilities(): util.Set[TableCapability] =
    Set(TableCapability.BATCH_READ).asJava

  override def newScanBuilder(scanOptions: CaseInsensitiveStringMap): ScanBuilder =
    new Oracle11ScanBuilder(options, tableSchema)
}
