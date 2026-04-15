package com.company.spark.oracle11

import org.apache.spark.sql.connector.read.{Batch, Scan}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

final class Oracle11Scan(
    options: Oracle11Options,
    fullSchema: StructType,
    requiredSchema: StructType,
    pushedFilters: Array[Filter],
    pushedPredicate: Option[Oracle11SqlPredicate],
    pushedLimit: Option[Int])
    extends Scan {

  override def readSchema(): StructType = requiredSchema

  override def toBatch(): Batch =
    new Oracle11Batch(
      options = options,
      fullSchema = fullSchema,
      requiredSchema = requiredSchema,
      pushedPredicate = pushedPredicate,
      pushedLimit = pushedLimit
    )

  override def description(): String = {
    val filters = if (pushedFilters.isEmpty) "none" else pushedFilters.mkString(",")
    val limit = pushedLimit.map(_.toString).getOrElse("none")
    s"Oracle11Scan(relation=${options.relation.description}, pushedFilters=$filters, pushedLimit=$limit)"
  }
}
