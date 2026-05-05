package com.company.spark.oracle11

import java.math.{BigDecimal => JBigDecimal}

import org.apache.spark.sql.types.DecimalType
import org.scalatest.funsuite.AnyFunSuite

final class Oracle11JdbcUtilsSuite extends AnyFunSuite {

  test("toSparkDecimal keeps requested precision/scale for in-range values") {
    val target = DecimalType(10, 2)
    val decimal = Oracle11JdbcUtils.toSparkDecimal(new JBigDecimal("123.45"), target)

    assert(decimal != null)
    assert(decimal.precision == target.precision)
    assert(decimal.scale == 2)
    assert(decimal.toJavaBigDecimal.compareTo(new JBigDecimal("123.45")) == 0)
  }

  test("toSparkDecimal falls back for scale overflow without throwing") {
    val target = DecimalType(10, 2)
    val decimal = Oracle11JdbcUtils.toSparkDecimal(new JBigDecimal("1.2345"), target)

    assert(decimal != null)
    assert(decimal.toJavaBigDecimal.compareTo(new JBigDecimal("1.2345")) == 0)
    assert(decimal.scale == 4)
  }

  test("toSparkDecimal falls back for precision overflow without throwing") {
    val target = DecimalType(10, 2)
    val decimal = Oracle11JdbcUtils.toSparkDecimal(new JBigDecimal("1234567890123.45"), target)

    assert(decimal != null)
    assert(decimal.toJavaBigDecimal.compareTo(new JBigDecimal("1234567890123.45")) == 0)
    assert(decimal.precision > target.precision)
  }
}
