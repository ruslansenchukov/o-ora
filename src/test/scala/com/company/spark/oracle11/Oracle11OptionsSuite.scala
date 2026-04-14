package com.company.spark.oracle11

import org.scalatest.funsuite.AnyFunSuite

final class Oracle11OptionsSuite extends AnyFunSuite {

  test("parse minimal dbtable options") {
    val options = Oracle11Options.fromMap(
      Map(
        "url" -> "jdbc:oracle:thin:@//localhost:1521/XE",
        "user" -> "hr",
        "password" -> "hr",
        "dbtable" -> "HR.EMPLOYEES"
      ))

    assert(options.dbtable.contains("HR.EMPLOYEES"))
    assert(options.query.isEmpty)
    assert(options.fetchSize == 1000)
    assert(options.partitioning.isEmpty)
  }

  test("require exactly one of dbtable or query") {
    val both = intercept[IllegalArgumentException] {
      Oracle11Options.fromMap(
        Map(
          "url" -> "u",
          "user" -> "a",
          "password" -> "b",
          "dbtable" -> "T",
          "query" -> "select 1 from dual"
        ))
    }
    assert(both.getMessage.contains("Exactly one"))

    val none = intercept[IllegalArgumentException] {
      Oracle11Options.fromMap(Map("url" -> "u", "user" -> "a", "password" -> "b"))
    }
    assert(none.getMessage.contains("Exactly one"))
  }

  test("reject partial partition options") {
    val error = intercept[IllegalArgumentException] {
      Oracle11Options.fromMap(
        Map(
          "url" -> "u",
          "user" -> "a",
          "password" -> "b",
          "dbtable" -> "T",
          "partitionColumn" -> "ID",
          "lowerBound" -> "1"
        ))
    }
    assert(error.getMessage.contains("must be provided together"))
  }

  test("parse complete partition options") {
    val options = Oracle11Options.fromMap(
      Map(
        "url" -> "u",
        "user" -> "a",
        "password" -> "b",
        "dbtable" -> "T",
        "partitionColumn" -> "ID",
        "lowerBound" -> "1",
        "upperBound" -> "100",
        "numPartitions" -> "4",
        "fetchsize" -> "2000"
      ))

    assert(options.fetchSize == 2000)
    assert(options.partitioning.nonEmpty)
    val p = options.partitioning.get
    assert(p.partitionColumn == "ID")
    assert(p.lowerBound == "1")
    assert(p.upperBound == "100")
    assert(p.numPartitions == 4)
  }
}
