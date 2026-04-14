package com.company.spark.oracle11

import java.sql.Types

import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite

final class Oracle11TypeMapperSuite extends AnyFunSuite {

  test("map Oracle string-like types") {
    assert(Oracle11TypeMapper.toSparkType(Types.VARCHAR, 0, 0, "VARCHAR2") == StringType)
    assert(Oracle11TypeMapper.toSparkType(Types.CHAR, 0, 0, "NCHAR") == StringType)
  }

  test("map Oracle large object and binary types") {
    assert(Oracle11TypeMapper.toSparkType(Types.CLOB, 0, 0, "CLOB") == StringType)
    assert(Oracle11TypeMapper.toSparkType(Types.BLOB, 0, 0, "BLOB") == BinaryType)
    assert(Oracle11TypeMapper.toSparkType(Types.VARBINARY, 0, 0, "RAW") == BinaryType)
  }

  test("map NUMBER with scale 0") {
    assert(Oracle11TypeMapper.toSparkType(Types.NUMERIC, 9, 0, "NUMBER") == IntegerType)
    assert(Oracle11TypeMapper.toSparkType(Types.NUMERIC, 18, 0, "NUMBER") == LongType)
    assert(Oracle11TypeMapper.toSparkType(Types.NUMERIC, 25, 0, "NUMBER") == DecimalType(25, 0))
    assert(Oracle11TypeMapper.toSparkType(Types.NUMERIC, 0, 0, "NUMBER") == DecimalType(38, 0))
  }

  test("map NUMBER with positive and negative scale") {
    assert(Oracle11TypeMapper.toSparkType(Types.NUMERIC, 12, 2, "NUMBER") == DecimalType(12, 2))
    assert(Oracle11TypeMapper.toSparkType(Types.NUMERIC, 0, 2, "NUMBER") == DoubleType)
    assert(Oracle11TypeMapper.toSparkType(Types.NUMERIC, 5, -2, "NUMBER") == DecimalType(7, 0))
  }

  test("map DATE and TIMESTAMP") {
    assert(Oracle11TypeMapper.toSparkType(Types.DATE, 0, 0, "DATE") == TimestampType)
    assert(Oracle11TypeMapper.toSparkType(Types.TIMESTAMP, 0, 0, "TIMESTAMP") == TimestampType)
  }
}
