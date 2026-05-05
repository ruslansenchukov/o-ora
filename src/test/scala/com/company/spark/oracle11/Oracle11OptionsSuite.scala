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
    assert(options.connectTimeoutMs.isEmpty)
    assert(options.readTimeoutMs.isEmpty)
    assert(options.queryTimeoutSec.isEmpty)
    assert(options.maxInListSize == 1000)
    assert(options.schemaCacheTtlSec == 300)
    assert(options.connectString == "//localhost:1521/XE")
    assert(options.autoPartitionMinRowsPerPartition == 100000L)
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

  test("reject partial manual partition options") {
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

  test("parse complete manual partition options") {
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
    assert(p.lowerBound.contains("1"))
    assert(p.upperBound.contains("100"))
    assert(p.numPartitions == 4)
    assert(!p.autoBounds)
  }

  test("auto bounds is disabled in native mode") {
    val error = intercept[IllegalArgumentException] {
      Oracle11Options.fromMap(
        Map(
          "url" -> "u",
          "user" -> "a",
          "password" -> "b",
          "dbtable" -> "T",
          "partitionColumn" -> "ID",
          "numPartitions" -> "4",
          "autoPartitionBounds" -> "true",
          "lowerBound" -> "1"
        ))
    }

    assert(error.getMessage.contains("not supported"))
  }

  test("parse host/port/serviceName contract with oracle aliases") {
    val options = Oracle11Options.fromMap(
      Map(
        "oracle.host" -> "db.example.local",
        "oracle.port" -> "1523",
        "oracle.serviceName" -> "XE",
        "oracle.user" -> "app_user",
        "oracle.password" -> "app_pass",
        "query" -> "select 1 from dual"
      ))

    assert(options.user == "app_user")
    assert(options.password == "app_pass")
    assert(options.url.startsWith("oci://db.example.local:1523"))
    assert(options.connectString.contains("HOST=db.example.local"))
    assert(options.connectString.contains("PORT=1523"))
    assert(options.connectString.contains("SERVICE_NAME=XE"))
  }

  test("reject serviceName + sid together") {
    val error = intercept[IllegalArgumentException] {
      Oracle11Options.fromMap(
        Map(
          "oracle.host" -> "db.example.local",
          "oracle.port" -> "1523",
          "oracle.serviceName" -> "XE",
          "oracle.sid" -> "XE",
          "oracle.user" -> "app_user",
          "oracle.password" -> "app_pass",
          "dbtable" -> "APP_USER.ORDERS_V2"
        ))
    }
    assert(error.getMessage.contains("mutually exclusive"))
  }

  test("parse timeout and tuning options") {
    val options = Oracle11Options.fromMap(
      Map(
        "url" -> "u",
        "user" -> "a",
        "password" -> "b",
        "dbtable" -> "T",
        "connectTimeoutMs" -> "3000",
        "readTimeoutMs" -> "8000",
        "queryTimeoutSec" -> "20",
        "maxInListSize" -> "500",
        "schemaCacheTtlSec" -> "0"
      ))

    assert(options.connectTimeoutMs.contains(3000))
    assert(options.readTimeoutMs.contains(8000))
    assert(options.queryTimeoutSec.contains(20))
    assert(options.maxInListSize == 500)
    assert(options.schemaCacheTtlSec == 0)
  }

  test("reject non-positive maxInListSize") {
    val error = intercept[IllegalArgumentException] {
      Oracle11Options.fromMap(
        Map(
          "url" -> "u",
          "user" -> "a",
          "password" -> "b",
          "dbtable" -> "T",
          "maxInListSize" -> "0"
        ))
    }

    assert(error.getMessage.contains("maxInListSize"))
  }
}
