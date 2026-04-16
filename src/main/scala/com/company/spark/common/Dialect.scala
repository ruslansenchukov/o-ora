package com.company.spark.common

import org.apache.spark.sql.sources.Filter

trait Dialect {
  def name: String
  def compileFilter(filter: Filter): Option[String]
  def quoteIdentifier(col: String): String
}
