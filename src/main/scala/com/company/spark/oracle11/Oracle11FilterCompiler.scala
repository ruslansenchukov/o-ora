package com.company.spark.oracle11

import java.util.Locale

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType

final case class Oracle11FilterCompilationResult(
    predicate: Option[Oracle11SqlPredicate],
    pushed: Array[Filter],
    unhandled: Array[Filter])

object Oracle11FilterCompiler {

  // Filter compiler is isolated to keep SQL generation safe and centralized.
  def compile(
      filters: Array[Filter],
      schema: StructType,
      maxInListSize: Int = 1000): Oracle11FilterCompilationResult = {

    if (maxInListSize <= 0) {
      throw new IllegalArgumentException(s"maxInListSize must be > 0, got $maxInListSize")
    }

    val safeFilters = Option(filters).getOrElse(Array.empty[Filter])
    val resolved = schema.fields.map(f => f.name.toLowerCase(Locale.ROOT) -> f).toMap
    val dialect = OracleDialect
    val compiler = new OracleFilterCompiler(
      schemaIndex = resolved,
      maxInListSize = maxInListSize,
      dialect = dialect
    )

    val handled = ArrayBuffer.empty[Oracle11SqlPredicate]
    val pushed = ArrayBuffer.empty[Filter]
    val unhandled = ArrayBuffer.empty[Filter]

    safeFilters.foreach { filter =>
      compiler.compilePredicate(filter) match {
        case Some(compiled) =>
          handled += compiled
          pushed += filter
        case None =>
          unhandled += filter
      }
    }

    val predicate = if (handled.nonEmpty) {
      val sql = handled.map(_.sql).mkString("(", ") AND (", ")")
      Some(Oracle11SqlPredicate(sql, handled.flatMap(_.params).toSeq))
    } else {
      None
    }

    Oracle11FilterCompilationResult(predicate, pushed.toArray, unhandled.toArray)
  }
}
