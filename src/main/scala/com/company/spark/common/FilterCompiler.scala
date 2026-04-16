package com.company.spark.common

import org.apache.spark.sql.sources.Filter

trait FilterCompiler {
  def compile(filter: Filter): Option[String]
}
