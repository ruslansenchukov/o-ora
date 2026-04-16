package com.company.spark.oracle11

import org.scalatest.funsuite.AnyFunSuite

final class OracleDialectSuite extends AnyFunSuite {

  test("quoteIdentifier matches existing Oracle quoting behavior") {
    assert(OracleDialect.quoteIdentifier("ID") == "\"ID\"")
    assert(OracleDialect.quoteIdentifier("A\"B") == "\"A\"\"B\"")
  }
}
