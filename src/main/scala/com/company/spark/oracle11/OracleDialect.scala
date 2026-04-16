package com.company.spark.oracle11

import com.company.spark.common.Dialect
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructField

object OracleDialect extends Dialect {
  override val name: String = "oracle11"

  // Context-free compilation is intentionally conservative; runtime path uses schema-aware method below.
  override def compileFilter(filter: Filter): Option[String] = None

  override def quoteIdentifier(col: String): String = Oracle11JdbcUtils.quoteIdentifier(col)

  def compileFilter(
      filter: Filter,
      schemaIndex: Map[String, StructField],
      maxInListSize: Int): Option[Oracle11SqlPredicate] =
    OracleFilterCompiler.compilePredicate(filter, schemaIndex, maxInListSize, this)
}
