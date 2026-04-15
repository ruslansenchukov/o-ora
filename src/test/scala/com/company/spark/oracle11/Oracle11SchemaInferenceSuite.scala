package com.company.spark.oracle11

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

final class Oracle11SchemaInferenceSuite extends AnyFunSuite with BeforeAndAfterEach {

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    Oracle11SchemaInference.clearCacheForTests()
  }

  override protected def afterEach(): Unit = {
    Oracle11SchemaInference.clearCacheForTests()
    super.afterEach()
  }

  private def options(ttlSec: Int): Oracle11Options =
    Oracle11Options.fromMap(
      Map(
        "url" -> "jdbc:oracle:thin:@//localhost:1521/XE",
        "user" -> "hr",
        "password" -> "hr",
        "dbtable" -> "HR.EMPLOYEES",
        "schemaCacheTtlSec" -> ttlSec.toString
      ))

  private val sampleSchema = StructType(
    Seq(
      StructField("ID", IntegerType, nullable = false),
      StructField("NAME", StringType, nullable = true)
    ))

  test("cache hit reuses inferred schema within ttl") {
    val opts = options(ttlSec = 300)
    var loadCount = 0

    val first = Oracle11SchemaInference.inferWithLoader(opts) {
      loadCount += 1
      sampleSchema
    }
    val second = Oracle11SchemaInference.inferWithLoader(opts) {
      loadCount += 1
      throw new IllegalStateException("cache miss should not happen")
    }

    assert(first == sampleSchema)
    assert(second == sampleSchema)
    assert(loadCount == 1)
  }

  test("cache expires after ttl") {
    val opts = options(ttlSec = 1)
    var loadCount = 0

    Oracle11SchemaInference.inferWithLoader(opts) {
      loadCount += 1
      sampleSchema
    }

    Thread.sleep(1200L)

    Oracle11SchemaInference.inferWithLoader(opts) {
      loadCount += 1
      sampleSchema
    }

    assert(loadCount == 2)
  }

  test("cache disabled when ttl <= 0") {
    val opts = options(ttlSec = 0)
    var loadCount = 0

    Oracle11SchemaInference.inferWithLoader(opts) {
      loadCount += 1
      sampleSchema
    }
    Oracle11SchemaInference.inferWithLoader(opts) {
      loadCount += 1
      sampleSchema
    }

    assert(loadCount == 2)
  }

  test("failed schema inference is not cached") {
    val opts = options(ttlSec = 300)
    var loadCount = 0

    intercept[RuntimeException] {
      Oracle11SchemaInference.inferWithLoader(opts) {
        loadCount += 1
        throw new RuntimeException("boom")
      }
    }

    val loaded = Oracle11SchemaInference.inferWithLoader(opts) {
      loadCount += 1
      sampleSchema
    }

    assert(loaded == sampleSchema)
    assert(loadCount == 2)
  }
}
