package com.company.spark.oracle11

import org.apache.spark.sql.types.{IntegerType, StructType}

final case class Oracle11BuiltQuery(sql: String, params: Seq[JdbcParameter])

object Oracle11QueryBuilder {

  def buildSelect(
      relation: Oracle11Relation,
      requiredSchema: StructType,
      partitionPredicate: Option[Oracle11SqlPredicate],
      pushedPredicate: Option[Oracle11SqlPredicate],
      limit: Option[Int] = None): Oracle11BuiltQuery = {

    val dialect = OracleDialect
    val projection = if (requiredSchema.isEmpty) {
      "1"
    } else {
      requiredSchema.fields.map(f => dialect.quoteIdentifier(f.name)).mkString(", ")
    }

    val predicates = Seq(partitionPredicate, pushedPredicate).flatten
    val whereClause =
      if (predicates.nonEmpty) {
        s" WHERE ${predicates.map(p => s"(${p.sql})").mkString(" AND ")}"
      } else {
        ""
      }

    val baseSql = s"SELECT $projection FROM ${relation.fromClause}$whereClause"
    val baseParams = predicates.flatMap(_.params)

    val (sql, params) = limit match {
      // Oracle 11 does not support ANSI FETCH FIRST, so we keep a rownum wrapper.
      case Some(value) if value > 0 =>
        (
          s"SELECT * FROM ($baseSql) ORA11_LIMIT WHERE ROWNUM <= ?",
          baseParams :+ JdbcParameter(value, IntegerType)
        )
      case _ =>
        (baseSql, baseParams)
    }

    Oracle11BuiltQuery(sql, params)
  }
}
