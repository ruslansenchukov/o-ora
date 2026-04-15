package com.company.spark.oracle11

sealed trait Oracle11Relation extends Serializable {
  def fromClause: String
  def description: String
  def cacheKey: String
}

final case class Oracle11TableRelation(tableOrExpression: String) extends Oracle11Relation {
  override val fromClause: String = tableOrExpression
  override val description: String = s"table=$tableOrExpression"
  override val cacheKey: String = s"table:$tableOrExpression"
}

final case class Oracle11QueryRelation(querySql: String) extends Oracle11Relation {
  override val fromClause: String = s"($querySql) ORA11_QUERY"
  override val description: String = "query"
  override val cacheKey: String = s"query:$querySql"
}

object Oracle11Relation {
  def fromOptions(options: Oracle11Options): Oracle11Relation = {
    options.query match {
      case Some(q) => Oracle11QueryRelation(q)
      case None    => Oracle11TableRelation(options.dbtable.get)
    }
  }
}
