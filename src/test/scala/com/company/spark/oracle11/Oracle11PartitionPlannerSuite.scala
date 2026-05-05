package com.company.spark.oracle11

import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite

final class Oracle11PartitionPlannerSuite extends AnyFunSuite {

  private val base = Map(
    "url" -> "jdbc:oracle:thin:@//localhost:1521/XE",
    "user" -> "u",
    "password" -> "p",
    "dbtable" -> "HR.EMPLOYEES"
  )

  test("single partition when partitioning options are absent") {
    val options = Oracle11Options.fromMap(base)
    val schema = StructType(Seq(StructField("ID", IntegerType, nullable = false)))

    val partitions = Oracle11PartitionPlanner.plan(options, options.relation, schema, pushedPredicate = None)
    assert(partitions.length == 1)
    assert(partitions.head.predicate.isEmpty)
  }

  test("plan numeric range partitions") {
    val options = Oracle11Options.fromMap(
      base ++ Map(
        "partitionColumn" -> "ID",
        "lowerBound" -> "0",
        "upperBound" -> "90",
        "numPartitions" -> "3"
      ))

    val schema = StructType(Seq(StructField("ID", IntegerType, nullable = true)))
    val partitions = Oracle11PartitionPlanner.plan(options, options.relation, schema, pushedPredicate = None)

    assert(partitions.length == 3)
    assert(partitions.head.predicate.exists(_.sql.contains("OR \"ID\" IS NULL")))
    assert(partitions(1).predicate.exists(_.sql.contains("\"ID\" >= ? AND \"ID\" < ?")))
    assert(partitions(2).predicate.exists(_.sql.contains("\"ID\" >= ?")))
    assert(partitions.head.predicate.get.params.head.sparkType == IntegerType)
    assert(partitions(1).predicate.get.params.forall(_.sparkType == IntegerType))
  }

  test("plan decimal range partitions keeps decimal bind type") {
    val options = Oracle11Options.fromMap(
      base ++ Map(
        "partitionColumn" -> "AMOUNT",
        "lowerBound" -> "0",
        "upperBound" -> "1000",
        "numPartitions" -> "4"
      ))

    val decimalType = DecimalType(12, 2)
    val schema = StructType(Seq(StructField("AMOUNT", decimalType, nullable = true)))
    val partitions = Oracle11PartitionPlanner.plan(options, options.relation, schema, pushedPredicate = None)

    assert(partitions.length == 4)
    assert(partitions.head.predicate.get.params.head.sparkType == decimalType)
    assert(partitions(1).predicate.get.params.forall(_.sparkType == decimalType))
  }

  test("plan timestamp range partitions") {
    val options = Oracle11Options.fromMap(
      base ++ Map(
        "partitionColumn" -> "CREATED_AT",
        "lowerBound" -> "2020-01-01 00:00:00",
        "upperBound" -> "2020-01-04 00:00:00",
        "numPartitions" -> "3"
      ))

    val schema = StructType(Seq(StructField("CREATED_AT", TimestampType, nullable = true)))
    val partitions = Oracle11PartitionPlanner.plan(options, options.relation, schema, pushedPredicate = None)

    assert(partitions.length == 3)
    assert(partitions.forall(_.predicate.nonEmpty))
    assert(partitions.head.predicate.get.params.head.sparkType == TimestampType)
  }

  test("plan date range partitions with DateType boundaries") {
    val options = Oracle11Options.fromMap(
      base ++ Map(
        "partitionColumn" -> "DT",
        "lowerBound" -> "2020-01-01",
        "upperBound" -> "2020-01-04",
        "numPartitions" -> "3"
      ))

    val schema = StructType(Seq(StructField("DT", DateType, nullable = true)))
    val partitions = Oracle11PartitionPlanner.plan(options, options.relation, schema, pushedPredicate = None)

    assert(partitions.length == 3)
    assert(partitions.head.predicate.get.params.head.sparkType == DateType)
  }

  test("fail on unknown partition column") {
    val options = Oracle11Options.fromMap(
      base ++ Map(
        "partitionColumn" -> "MISSING",
        "lowerBound" -> "0",
        "upperBound" -> "10",
        "numPartitions" -> "2"
      ))

    val schema = StructType(Seq(StructField("ID", IntegerType, nullable = true)))
    val err = intercept[IllegalArgumentException] {
      Oracle11PartitionPlanner.plan(options, options.relation, schema, pushedPredicate = None)
    }
    assert(err.getMessage.contains("was not found"))
  }

}
