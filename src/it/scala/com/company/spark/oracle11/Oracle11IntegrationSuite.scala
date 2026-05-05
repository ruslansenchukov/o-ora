package com.company.spark.oracle11

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{BinaryType, IntegerType, StringType, TimestampType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

final class Oracle11IntegrationSuite extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private var target: Oracle11IntegrationTarget = _
  private val tableName = "ORA11_CONNECTOR_IT"
  private val legacyJdbcEnabled = sys.props.get("oracle11.jdbc.it.enabled").exists(_.toBoolean)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    if (legacyJdbcEnabled) {
      spark = SparkSession.builder().appName("oracle11-it").master("local[2]").getOrCreate()
      target = Oracle11IntegrationHarness.resolve().orNull
      if (target != null) {
        Oracle11IntegrationHarness.initializeTable(target, tableName)
      }
    }
  }

  override protected def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
    if (target != null) {
      target.close()
    }
    super.afterAll()
  }

  private def requireTarget(): Oracle11IntegrationTarget = {
    if (!legacyJdbcEnabled) {
      cancel(
        "Legacy JDBC-oriented IT suite is disabled by default. " +
          "Enable with -Doracle11.jdbc.it.enabled=true only when JDBC test dependencies are available.")
    }

    if (target == null) {
      cancel(
        "Integration target is not configured. Use -Doracle11.it.enabled=true and optionally " +
          "ORA11_IT_URL/ORA11_IT_USER/ORA11_IT_PASSWORD")
    }
    target
  }

  test("read table via oracle11 datasource") {
    val it = requireTarget()

    val df = spark.read
      .format("oracle11")
      .option("url", it.url)
      .option("dbtable", tableName)
      .option("user", it.user)
      .option("password", it.password)
      .load()

    assert(df.count() == 3)

    val schema = df.schema
    assert(schema("ID").dataType == IntegerType)
    assert(schema("NAME").dataType == StringType)
    assert(schema("CREATED_AT").dataType == TimestampType)
    assert(schema("PAYLOAD").dataType == BinaryType)
  }

  test("filter and column pruning work end-to-end") {
    val it = requireTarget()

    val rows = spark.read
      .format("oracle11")
      .option("url", it.url)
      .option("dbtable", tableName)
      .option("user", it.user)
      .option("password", it.password)
      .load()
      .select("ID", "NAME")
      .where("ID >= 2")
      .collect()

    assert(rows.length == 2)
    assert(rows.map(_.getInt(0)).sorted.sameElements(Array(2, 3)))
  }

  test("range partitioning reads complete dataset") {
    val it = requireTarget()

    val df = spark.read
      .format("oracle11")
      .option("url", it.url)
      .option("dbtable", tableName)
      .option("user", it.user)
      .option("password", it.password)
      .option("partitionColumn", "ID")
      .option("lowerBound", "1")
      .option("upperBound", "4")
      .option("numPartitions", "3")
      .load()

    assert(df.count() == 3)
    assert(df.rdd.getNumPartitions == 3)
  }

  test("query mode works") {
    val it = requireTarget()

    val df = spark.read
      .format("oracle11")
      .option("url", it.url)
      .option("query", s"SELECT ID, NAME FROM $tableName")
      .option("user", it.user)
      .option("password", it.password)
      .load()

    assert(df.schema("ID").dataType == IntegerType)
    assert(df.schema("NAME").dataType == StringType)
    assert(df.count() == 3)
  }

  test("limit pushdown preserves result and appears in plan") {
    val it = requireTarget()

    val df = spark.read
      .format("oracle11")
      .option("url", it.url)
      .option("dbtable", tableName)
      .option("user", it.user)
      .option("password", it.password)
      .load()
      .limit(2)

    assert(df.count() == 2)
    val scanDescriptions = df.queryExecution.executedPlan.collect {
      case scan: BatchScanExec => scan.scan.description()
    }
    val executedPlan = df.queryExecution.executedPlan.toString()
    assert(
      scanDescriptions.exists(_.contains("pushedLimit=2")) ||
        executedPlan.contains("pushedLimit=2") ||
        executedPlan.contains("PushedLimit"),
      s"Expected pushed limit marker in scan description, got:\n${scanDescriptions.mkString("\n")}\n$executedPlan"
    )
  }

  test("auto bounds partitioning reads complete dataset") {
    val it = requireTarget()

    val df = spark.read
      .format("oracle11")
      .option("url", it.url)
      .option("dbtable", tableName)
      .option("user", it.user)
      .option("password", it.password)
      .option("partitionColumn", "ID")
      .option("numPartitions", "10")
      .option("autoPartitionBounds", "true")
      .option("autoPartitionMinRowsPerPartition", "1")
      .load()

    assert(df.count() == 3)
    val partitions = df.rdd.getNumPartitions
    assert(partitions >= 1 && partitions <= 10)
    assert(partitions > 1)
  }

  test("large IN predicate does not fail with ORA-01795") {
    val it = requireTarget()
    val inValues = (1 to 1205).map(Int.box).toSeq

    val df = spark.read
      .format("oracle11")
      .option("url", it.url)
      .option("dbtable", tableName)
      .option("user", it.user)
      .option("password", it.password)
      .load()
      .where(col("ID").isin(inValues: _*))

    assert(df.count() == 3)

    val scanDescriptions = df.queryExecution.executedPlan.collect {
      case scan: BatchScanExec => scan.scan.description()
    }
    val executedPlan = df.queryExecution.executedPlan.toString()
    assert(
      scanDescriptions.exists(_.contains("In(ID")) ||
        executedPlan.contains("In(") ||
        executedPlan.contains("InSet"),
      s"Expected pushed IN filter in scan description, got:\n${scanDescriptions.mkString("\n")}\n$executedPlan"
    )
  }
}
