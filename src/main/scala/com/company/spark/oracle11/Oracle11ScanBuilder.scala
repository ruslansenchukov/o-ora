package com.company.spark.oracle11

import java.util.Locale

import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownFilters, SupportsPushDownLimit, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{StructField, StructType}

final class Oracle11ScanBuilder(options: Oracle11Options, fullSchema: StructType)
    extends ScanBuilder
    with SupportsPushDownRequiredColumns
    with SupportsPushDownFilters
    with SupportsPushDownLimit {

  private val schemaIndex: Map[String, StructField] =
    fullSchema.fields.map(f => f.name.toLowerCase(Locale.ROOT) -> f).toMap

  private var requiredSchema: StructType = fullSchema
  private var pushedFiltersState: Array[Filter] = Array.empty
  private var pushedPredicateState: Option[Oracle11SqlPredicate] = None
  private var pushedLimitState: Option[Int] = None
  private var partiallyPushedLimitState = false

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
    val compiled = Oracle11FilterCompiler.compile(filters, fullSchema, options.maxInListSize)
    pushedFiltersState = compiled.pushed
    pushedPredicateState = compiled.predicate
    compiled.unhandled
  }

  override def pushedFilters(): Array[Filter] = pushedFiltersState

  override def pushLimit(limit: Int): Boolean = {
    if (limit <= 0) {
      false
    } else {
      pushedLimitState = Some(pushedLimitState.map(current => math.min(current, limit)).getOrElse(limit))
      partiallyPushedLimitState = options.partitioning.exists(_.numPartitions > 1)
      true
    }
  }

  override def isPartiallyPushed(): Boolean = partiallyPushedLimitState

  override def build(): Scan = {
    new Oracle11Scan(
      options = options,
      fullSchema = fullSchema,
      requiredSchema = requiredSchema,
      pushedFilters = pushedFiltersState,
      pushedPredicate = pushedPredicateState,
      pushedLimit = pushedLimitState
    )
  }
}
