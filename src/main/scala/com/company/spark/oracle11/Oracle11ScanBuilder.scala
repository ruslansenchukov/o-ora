package com.company.spark.oracle11

import java.util.Locale

import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.apache.spark.sql.connector.read.SupportsPushDownFilters
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{StructField, StructType}

final class Oracle11ScanBuilder(options: Oracle11Options, fullSchema: StructType)
    extends ScanBuilder
    with SupportsPushDownRequiredColumns
    with SupportsPushDownFilters {

  private val schemaIndex: Map[String, StructField] =
    fullSchema.fields.map(f => f.name.toLowerCase(Locale.ROOT) -> f).toMap

  private var requiredSchema: StructType = fullSchema
  private var pushedFiltersState: Array[Filter] = Array.empty
  private var pushedPredicateState: Option[Oracle11SqlPredicate] = None

  override def pruneColumns(requestedSchema: StructType): Unit = {
    if (requestedSchema == null || requestedSchema.isEmpty) {
      requiredSchema = StructType(Nil)
      return
    }

    val resolvedFields = requestedSchema.fieldNames.map { name =>
      schemaIndex.get(name.toLowerCase(Locale.ROOT)).getOrElse {
        throw new IllegalArgumentException(s"Unknown required column '$name'")
      }
    }
    requiredSchema = StructType(resolvedFields)
  }

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    val compiled = Oracle11FilterCompiler.compile(filters, fullSchema)
    pushedFiltersState = compiled.pushed
    pushedPredicateState = compiled.predicate
    compiled.unhandled
  }

  override def pushedFilters(): Array[Filter] = pushedFiltersState

  override def build(): Scan = {
    new Oracle11Scan(
      options = options,
      fullSchema = fullSchema,
      requiredSchema = requiredSchema,
      pushedFilters = pushedFiltersState,
      pushedPredicate = pushedPredicateState
    )
  }
}
