package com.company.spark.oracle11

import java.math.MathContext
import java.sql.Timestamp
import java.time.ZoneOffset

import org.apache.spark.sql.types._

object Oracle11PartitionPlanner {

  // Partition planning mirrors Spark JDBC semantics: bounds define stride, not hard clipping.
  def plan(options: Oracle11Options, schema: StructType): Array[Oracle11InputPartition] = {
    options.partitioning match {
      case None =>
        Array(Oracle11InputPartition(None))

      case Some(partitioning) =>
        val field = schema.fields
          .find(_.name.equalsIgnoreCase(partitioning.partitionColumn))
          .getOrElse {
            val available = schema.fieldNames.mkString(", ")
            throw new IllegalArgumentException(
              s"Partition column '${partitioning.partitionColumn}' was not found in schema. Available: [$available]")
          }

        val columnSql = Oracle11JdbcUtils.quoteIdentifier(field.name)
        val numPartitions = partitioning.numPartitions
        if (numPartitions <= 1) {
          return Array(Oracle11InputPartition(None))
        }

        field.dataType match {
          case dt if isNumeric(dt) =>
            planNumeric(columnSql, partitioning.lowerBound, partitioning.upperBound, numPartitions)

          case DateType | TimestampType =>
            planTemporal(columnSql, partitioning.lowerBound, partitioning.upperBound, numPartitions)

          case other =>
            throw new IllegalArgumentException(
              s"Partition column '${field.name}' has unsupported type '$other'. " +
                "Only numeric/date/timestamp are supported")
        }
    }
  }

  private def isNumeric(dataType: DataType): Boolean = dataType match {
    case ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType | _: DecimalType => true
    case _                                                                                       => false
  }

  private def planNumeric(
      columnSql: String,
      lowerBoundRaw: String,
      upperBoundRaw: String,
      numPartitions: Int): Array[Oracle11InputPartition] = {

    val lower = parseBigDecimal(lowerBoundRaw, "lowerBound")
    val upper = parseBigDecimal(upperBoundRaw, "upperBound")

    val diff = upper - lower
    if (diff <= 0) {
      return Array(Oracle11InputPartition(None))
    }

    val stride = BigDecimal(
      diff.bigDecimal.divide(BigDecimal(numPartitions).bigDecimal, MathContext.DECIMAL128))
    if (stride <= 0) {
      return Array(Oracle11InputPartition(None))
    }

    val boundaries = (1 until numPartitions).map(i => lower + stride * i)
    buildPartitions(columnSql, boundaries.map(_.bigDecimal), DecimalType(38, 18))
  }

  private def planTemporal(
      columnSql: String,
      lowerBoundRaw: String,
      upperBoundRaw: String,
      numPartitions: Int): Array[Oracle11InputPartition] = {

    val lowerTs = Oracle11JdbcUtils.parseTimestampBound(lowerBoundRaw)
    val upperTs = Oracle11JdbcUtils.parseTimestampBound(upperBoundRaw)

    val lowerMs = lowerTs.toInstant.atZone(ZoneOffset.UTC).toInstant.toEpochMilli
    val upperMs = upperTs.toInstant.atZone(ZoneOffset.UTC).toInstant.toEpochMilli
    val diff = upperMs - lowerMs

    if (diff <= 0L) {
      return Array(Oracle11InputPartition(None))
    }

    val stride = diff / numPartitions
    if (stride <= 0L) {
      return Array(Oracle11InputPartition(None))
    }

    val boundaries = (1 until numPartitions).map { i =>
      new Timestamp(lowerMs + stride * i)
    }

    buildPartitions(columnSql, boundaries, TimestampType)
  }

  private def buildPartitions(
      columnSql: String,
      boundaries: Seq[Any],
      boundaryType: DataType): Array[Oracle11InputPartition] = {

    if (boundaries.isEmpty) {
      return Array(Oracle11InputPartition(None))
    }

    val first = Oracle11SqlPredicate(
      sql = s"($columnSql < ? OR $columnSql IS NULL)",
      params = Seq(JdbcParameter(boundaries.head, boundaryType))
    )

    val middle = boundaries.sliding(2).toSeq.map {
      case Seq(start, end) =>
        Oracle11SqlPredicate(
          sql = s"($columnSql >= ? AND $columnSql < ?)",
          params = Seq(JdbcParameter(start, boundaryType), JdbcParameter(end, boundaryType))
        )
      case _ =>
        throw new IllegalStateException("Unexpected partition boundary window")
    }

    val last = Oracle11SqlPredicate(
      sql = s"($columnSql >= ?)",
      params = Seq(JdbcParameter(boundaries.last, boundaryType))
    )

    (first +: middle :+ last)
      .map(p => Oracle11InputPartition(Some(p)))
      .toArray
  }

  private def parseBigDecimal(raw: String, optionName: String): BigDecimal = {
    val value = raw.trim
    try {
      BigDecimal(value)
    } catch {
      case _: NumberFormatException =>
        throw new IllegalArgumentException(s"Option '$optionName' must be numeric, got '$raw'")
    }
  }
}
