package com.company.spark.oracle11

import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite

final class Oracle11QueryBuilderSuite extends AnyFunSuite {

  test("build SELECT for table relation with predicates") {
    val schema = StructType(Seq(StructField("ID", IntegerType), StructField("NAME", StringType)))
    val partition = Oracle11SqlPredicate("\"ID\" >= ?", Seq(JdbcParameter(10, IntegerType)))
    val pushed = Oracle11SqlPredicate("\"NAME\" IS NOT NULL", Nil)

    val built = Oracle11QueryBuilder.buildSelect(
      relation = Oracle11TableRelation("HR.EMPLOYEES"),
      requiredSchema = schema,
      partitionPredicate = Some(partition),
      pushedPredicate = Some(pushed)
    )

    assert(built.sql.startsWith("SELECT \"ID\", \"NAME\" FROM HR.EMPLOYEES"))
    assert(built.sql.contains("WHERE"))
    assert(built.sql.contains("\"ID\" >= ?"))
    assert(built.sql.contains("\"NAME\" IS NOT NULL"))
    assert(built.params.length == 1)
  }

  test("build SELECT for query relation") {
    val schema = StructType(Seq(StructField("C1", IntegerType)))

    val built = Oracle11QueryBuilder.buildSelect(
      relation = Oracle11QueryRelation("SELECT 1 AS C1 FROM DUAL"),
      requiredSchema = schema,
      partitionPredicate = None,
      pushedPredicate = None
    )

    assert(built.sql.contains("FROM (SELECT 1 AS C1 FROM DUAL) ORA11_QUERY"))
  }

  test("build SELECT with empty projection") {
    val built = Oracle11QueryBuilder.buildSelect(
      relation = Oracle11TableRelation("HR.EMPLOYEES"),
      requiredSchema = StructType(Nil),
      partitionPredicate = None,
      pushedPredicate = None
    )

    assert(built.sql.startsWith("SELECT 1 FROM HR.EMPLOYEES"))
  }

  test("build SELECT with limit uses placeholder") {
    val schema = StructType(Seq(StructField("ID", IntegerType)))

    val built = Oracle11QueryBuilder.buildSelect(
      relation = Oracle11TableRelation("HR.EMPLOYEES"),
      requiredSchema = schema,
      partitionPredicate = None,
      pushedPredicate = None,
      limit = Some(25)
    )

    assert(built.sql.contains("ROWNUM <= ?"))
    assert(built.params.nonEmpty)
    assert(built.params.last.sparkType == IntegerType)
    assert(built.params.last.value == 25)
  }

  test("build SELECT keeps predicate params before limit param") {
    val schema = StructType(Seq(StructField("ID", IntegerType)))
    val pushed = Oracle11SqlPredicate("\"ID\" >= ?", Seq(JdbcParameter(10, IntegerType)))

    val built = Oracle11QueryBuilder.buildSelect(
      relation = Oracle11TableRelation("HR.EMPLOYEES"),
      requiredSchema = schema,
      partitionPredicate = None,
      pushedPredicate = Some(pushed),
      limit = Some(2)
    )

    assert(built.params.length == 2)
    assert(built.params.head.value == 10)
    assert(built.params.last.value == 2)
  }
}
