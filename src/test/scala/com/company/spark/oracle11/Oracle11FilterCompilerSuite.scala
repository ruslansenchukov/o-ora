package com.company.spark.oracle11

import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite

final class Oracle11FilterCompilerSuite extends AnyFunSuite {

  private val schema = StructType(
    Seq(
      StructField("ID", IntegerType, nullable = false),
      StructField("NAME", StringType, nullable = true),
      StructField("SALARY", DecimalType(12, 2), nullable = true)
    ))

  test("compile simple supported filters") {
    val result = Oracle11FilterCompiler.compile(
      Array(EqualTo("ID", 10), IsNotNull("NAME"), GreaterThan("SALARY", BigDecimal(1000))),
      schema)

    assert(result.unhandled.isEmpty)
    assert(result.pushed.length == 3)
    assert(result.predicate.nonEmpty)
    val sql = result.predicate.get.sql
    assert(sql.contains("\"ID\" = ?"))
    assert(sql.contains("\"NAME\" IS NOT NULL"))
    assert(sql.contains("\"SALARY\" > ?"))
    assert(result.predicate.get.params.length == 2)
  }

  test("return unhandled unsupported filters") {
    val result = Oracle11FilterCompiler.compile(Array(StringStartsWith("NAME", "A")), schema)
    assert(result.pushed.isEmpty)
    assert(result.unhandled.length == 1)
    assert(result.predicate.isEmpty)
  }

  test("compile logical AND/OR only when both branches are supported") {
    val andFilter = And(EqualTo("ID", 1), LessThan("ID", 10))
    val orFilter = Or(EqualTo("ID", 1), EqualTo("NAME", "x"))

    val andResult = Oracle11FilterCompiler.compile(Array(andFilter), schema)
    assert(andResult.unhandled.isEmpty)
    assert(andResult.predicate.nonEmpty)
    assert(andResult.predicate.get.sql.contains("AND"))

    val orResult = Oracle11FilterCompiler.compile(Array(orFilter), schema)
    assert(orResult.unhandled.isEmpty)
    assert(orResult.predicate.nonEmpty)
    assert(orResult.predicate.get.sql.contains("OR"))
  }

  test("IN filter with empty value list compiles to always-false") {
    val result = Oracle11FilterCompiler.compile(Array(In("ID", Array.empty[Any])), schema)
    assert(result.unhandled.isEmpty)
    assert(result.predicate.exists(_.sql.contains("1 = 0")))
  }

  test("IN filter is chunked when values exceed Oracle limit") {
    val values = (1 to 12).map(Int.box).toArray[Any]
    val result = Oracle11FilterCompiler.compile(Array(In("ID", values)), schema, maxInListSize = 5)

    assert(result.unhandled.isEmpty)
    val predicate = result.predicate.get
    assert(predicate.sql.contains(" OR "))
    assert(predicate.params.length == 12)
    assert(predicate.sql.contains("\"ID\" IN"))
  }

  test("reject non-positive maxInListSize") {
    val error = intercept[IllegalArgumentException] {
      Oracle11FilterCompiler.compile(Array(In("ID", Array(1, 2))), schema, maxInListSize = 0)
    }
    assert(error.getMessage.contains("maxInListSize"))
  }
}
