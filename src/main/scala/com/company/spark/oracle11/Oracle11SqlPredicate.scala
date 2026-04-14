package com.company.spark.oracle11

final case class JdbcParameter(value: Any, sparkType: org.apache.spark.sql.types.DataType) extends Serializable

final case class Oracle11SqlPredicate(sql: String, params: Seq[JdbcParameter]) extends Serializable

object Oracle11SqlPredicate {
  def and(left: Oracle11SqlPredicate, right: Oracle11SqlPredicate): Oracle11SqlPredicate =
    Oracle11SqlPredicate(s"(${left.sql}) AND (${right.sql})", left.params ++ right.params)

  def or(left: Oracle11SqlPredicate, right: Oracle11SqlPredicate): Oracle11SqlPredicate =
    Oracle11SqlPredicate(s"(${left.sql}) OR (${right.sql})", left.params ++ right.params)
}
