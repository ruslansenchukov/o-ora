package com.company.spark.oracle11

import java.util.Locale

import com.company.spark.common.FilterCompiler
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructField

final class OracleFilterCompiler(
    schemaIndex: Map[String, StructField],
    maxInListSize: Int,
    dialect: OracleDialect.type = OracleDialect)
    extends FilterCompiler {

  if (maxInListSize <= 0) {
    throw new IllegalArgumentException(s"maxInListSize must be > 0, got $maxInListSize")
  }

  override def compile(filter: Filter): Option[String] =
    compilePredicate(filter).map(_.sql)

  def compilePredicate(filter: Filter): Option[Oracle11SqlPredicate] =
    dialect.compileFilter(filter, schemaIndex, maxInListSize)
}

object OracleFilterCompiler {

  private[oracle11] def compilePredicate(
      filter: Filter,
      schemaIndex: Map[String, StructField],
      maxInListSize: Int,
      dialect: OracleDialect.type): Option[Oracle11SqlPredicate] = {

    def column(fieldName: String): Option[StructField] =
      schemaIndex.get(fieldName.toLowerCase(Locale.ROOT))

    def binary(attribute: String, op: String, value: Any): Option[Oracle11SqlPredicate] = {
      column(attribute).flatMap { field =>
        if (value == null) {
          None
        } else {
          Oracle11JdbcUtils.normalizeLiteral(value, field.dataType).map { normalized =>
            Oracle11SqlPredicate(
              sql = s"${dialect.quoteIdentifier(field.name)} $op ?",
              params = Seq(JdbcParameter(normalized, field.dataType))
            )
          }
        }
      }
    }

    filter match {
      case EqualTo(attribute, value) if value == null =>
        column(attribute).map { field =>
          Oracle11SqlPredicate(s"${dialect.quoteIdentifier(field.name)} IS NULL", Nil)
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
          val columnSql = dialect.quoteIdentifier(field.name)
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
          Oracle11SqlPredicate(s"${dialect.quoteIdentifier(field.name)} IS NULL", Nil)
        }

      case IsNotNull(attribute) =>
        column(attribute).map { field =>
          Oracle11SqlPredicate(s"${dialect.quoteIdentifier(field.name)} IS NOT NULL", Nil)
        }

      case And(left, right) =>
        for {
          l <- compilePredicate(left, schemaIndex, maxInListSize, dialect)
          r <- compilePredicate(right, schemaIndex, maxInListSize, dialect)
        } yield Oracle11SqlPredicate.and(l, r)

      case Or(left, right) =>
        for {
          l <- compilePredicate(left, schemaIndex, maxInListSize, dialect)
          r <- compilePredicate(right, schemaIndex, maxInListSize, dialect)
        } yield Oracle11SqlPredicate.or(l, r)

      case _ =>
        None
    }
  }
}
