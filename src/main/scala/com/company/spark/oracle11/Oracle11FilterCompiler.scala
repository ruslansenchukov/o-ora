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

    val handled = ArrayBuffer.empty[Oracle11SqlPredicate]
    val pushed = ArrayBuffer.empty[Filter]
    val unhandled = ArrayBuffer.empty[Filter]

    safeFilters.foreach { filter =>
      compileSingle(filter, resolved, maxInListSize) match {
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

  private def compileSingle(
      filter: Filter,
      schemaIndex: Map[String, org.apache.spark.sql.types.StructField],
      maxInListSize: Int): Option[Oracle11SqlPredicate] = {

    def column(fieldName: String): Option[org.apache.spark.sql.types.StructField] =
      schemaIndex.get(fieldName.toLowerCase(Locale.ROOT))

    def binary(attribute: String, op: String, value: Any): Option[Oracle11SqlPredicate] = {
      column(attribute).flatMap { field =>
        if (value == null) {
          None
        } else {
          Oracle11JdbcUtils.normalizeLiteral(value, field.dataType).map { normalized =>
            Oracle11SqlPredicate(
              sql = s"${Oracle11JdbcUtils.quoteIdentifier(field.name)} $op ?",
              params = Seq(JdbcParameter(normalized, field.dataType))
            )
          }
        }
      }
    }

    filter match {
      case EqualTo(attribute, value) if value == null =>
        column(attribute).map { field =>
          Oracle11SqlPredicate(s"${Oracle11JdbcUtils.quoteIdentifier(field.name)} IS NULL", Nil)
        }

      case EqualTo(attribute, value) =>
        binary(attribute, "=", value)

      case GreaterThan(attribute, value) =>
        binary(attribute, ">", value)

      case GreaterThanOrEqual(attribute, value) =>
        binary(attribute, ">=", value)

      case LessThan(attribute, value) =>
        binary(attribute, "<", value)

      case LessThanOrEqual(attribute, value) =>
        binary(attribute, "<=", value)

      case In(attribute, values) =>
        column(attribute).flatMap { field =>
          val columnSql = Oracle11JdbcUtils.quoteIdentifier(field.name)
          val normalized = values.toSeq
            .filter(_ != null)
            .flatMap(v => Oracle11JdbcUtils.normalizeLiteral(v, field.dataType))

          if (normalized.isEmpty) {
            Some(Oracle11SqlPredicate("1 = 0", Nil))
          } else {
            val groups = normalized.grouped(maxInListSize).toSeq
            val groupSql = groups.map { group =>
              val placeholders = group.map(_ => "?").mkString(", ")
              s"$columnSql IN ($placeholders)"
            }
            val sql = if (groupSql.length == 1) {
              groupSql.head
            } else {
              groupSql.map(s => s"($s)").mkString("(", " OR ", ")")
            }

            Some(
              Oracle11SqlPredicate(
                sql = sql,
                params = normalized.map(v => JdbcParameter(v, field.dataType))
              ))
          }
        }

      case IsNull(attribute) =>
        column(attribute).map { field =>
          Oracle11SqlPredicate(s"${Oracle11JdbcUtils.quoteIdentifier(field.name)} IS NULL", Nil)
        }

      case IsNotNull(attribute) =>
        column(attribute).map { field =>
          Oracle11SqlPredicate(s"${Oracle11JdbcUtils.quoteIdentifier(field.name)} IS NOT NULL", Nil)
        }

      case And(left, right) =>
        for {
          l <- compileSingle(left, schemaIndex, maxInListSize)
          r <- compileSingle(right, schemaIndex, maxInListSize)
        } yield Oracle11SqlPredicate.and(l, r)

      case Or(left, right) =>
        for {
          l <- compileSingle(left, schemaIndex, maxInListSize)
          r <- compileSingle(right, schemaIndex, maxInListSize)
        } yield Oracle11SqlPredicate.or(l, r)

      case _ =>
        None
    }
  }
}
