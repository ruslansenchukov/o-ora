package com.company.spark.oracle11

import java.math.MathContext
import java.sql.{Date, Timestamp}

import org.apache.spark.sql.types._

object Oracle11PartitionPlanner {
  // Partition planning mirrors Spark JDBC semantics: bounds define stride, not hard clipping.
  def plan(
      options: Oracle11Options,
      relation: Oracle11Relation,
      schema: StructType,
      pushedPredicate: Option[Oracle11SqlPredicate]): Array[Oracle11InputPartition] = {

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

        if (partitioning.numPartitions <= 1) {
          return Array(Oracle11InputPartition(None))
        }

        val columnSql = Oracle11JdbcUtils.quoteIdentifier(field.name)

        if (partitioning.autoBounds) {
          throw new IllegalArgumentException(
            "autoPartitionBounds is not supported in native OCI mode (v1). " +
              "Use manual lowerBound/upperBound.")
        }

        (partitioning.lowerBound, partitioning.upperBound) match {
          case (Some(lower), Some(upper)) =>
            planWithBounds(
              field = field,
              columnSql = columnSql,
              lowerBoundRaw = lower,
              upperBoundRaw = upper,
              numPartitions = partitioning.numPartitions
            )
          case _ =>
            throw new IllegalArgumentException(
              "Manual partitioning requires both lowerBound and upperBound")
        }
    }
  }

  private def planWithBounds(
      field: StructField,
      columnSql: String,
      lowerBoundRaw: String,
      upperBoundRaw: String,
      numPartitions: Int): Array[Oracle11InputPartition] = {

    field.dataType match {
      case dt if isNumeric(dt) =>
        val lower = parseBigDecimal(lowerBoundRaw, "lowerBound")
        val upper = parseBigDecimal(upperBoundRaw, "upperBound")
        planNumeric(columnSql, lower, upper, numPartitions, field.dataType)

      case DateType =>
        val lower = Oracle11JdbcUtils.parseDateBound(lowerBoundRaw)
        val upper = Oracle11JdbcUtils.parseDateBound(upperBoundRaw)
        planDate(columnSql, lower, upper, numPartitions)

      case TimestampType =>
        val lower = Oracle11JdbcUtils.parseTimestampBound(lowerBoundRaw)
        val upper = Oracle11JdbcUtils.parseTimestampBound(upperBoundRaw)
        planTimestamp(columnSql, lower, upper, numPartitions)

      case other =>
        throw new IllegalArgumentException(
          s"Partition column '${field.name}' has unsupported type '$other'. Only numeric/date/timestamp are supported")
    }
  }

  private def isNumeric(dataType: DataType): Boolean = dataType match {
    case ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType | _: DecimalType => true
    case _                                                                                       => false
  }

  private def planNumeric(
      columnSql: String,
      lower: BigDecimal,
      upper: BigDecimal,
      numPartitions: Int,
      numericType: DataType): Array[Oracle11InputPartition] = {

    val diff = upper - lower
    if (diff <= 0) {
      return Array(Oracle11InputPartition(None))
    }

    val stride = BigDecimal(
      diff.bigDecimal.divide(BigDecimal(numPartitions).bigDecimal, MathContext.DECIMAL128))
    if (stride <= 0) {
      return Array(Oracle11InputPartition(None))
    }

    val boundaryType = numericBoundaryType(numericType)
    val boundaries = (1 until numPartitions).map { i =>
      castNumericBoundary(lower + stride * i, boundaryType)
    }
    buildPartitions(columnSql, boundaries, boundaryType)
  }

  private def numericBoundaryType(dataType: DataType): DataType = dataType match {
    case ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType => dataType
    case d: DecimalType                                                           => d
    case _                                                                        => DecimalType(38, 18)
  }

  private def castNumericBoundary(boundary: BigDecimal, dataType: DataType): Any = dataType match {
    case ByteType =>
      safeToByte(boundary.setScale(0, BigDecimal.RoundingMode.FLOOR).toBigInt)
    case ShortType =>
      safeToShort(boundary.setScale(0, BigDecimal.RoundingMode.FLOOR).toBigInt)
    case IntegerType =>
      safeToInt(boundary.setScale(0, BigDecimal.RoundingMode.FLOOR).toBigInt)
    case LongType =>
      safeToLong(boundary.setScale(0, BigDecimal.RoundingMode.FLOOR).toBigInt)
    case FloatType =>
      boundary.toFloat
    case DoubleType =>
      boundary.toDouble
    case _: DecimalType =>
      boundary.bigDecimal
    case _ =>
      boundary.bigDecimal
  }

  private def safeToByte(value: BigInt): Byte = {
    if (value < BigInt(Byte.MinValue)) Byte.MinValue
    else if (value > BigInt(Byte.MaxValue)) Byte.MaxValue
    else value.toByte
  }

  private def safeToShort(value: BigInt): Short = {
    if (value < BigInt(Short.MinValue)) Short.MinValue
    else if (value > BigInt(Short.MaxValue)) Short.MaxValue
    else value.toShort
  }

  private def safeToInt(value: BigInt): Int = {
    if (value < BigInt(Int.MinValue)) Int.MinValue
    else if (value > BigInt(Int.MaxValue)) Int.MaxValue
    else value.toInt
  }

  private def safeToLong(value: BigInt): Long = {
    if (value < BigInt(Long.MinValue)) Long.MinValue
    else if (value > BigInt(Long.MaxValue)) Long.MaxValue
    else value.toLong
  }

  private def planDate(
      columnSql: String,
      lower: Date,
      upper: Date,
      numPartitions: Int): Array[Oracle11InputPartition] = {

    val lowerMs = lower.getTime
    val upperMs = upper.getTime
    val diff = upperMs - lowerMs

    if (diff <= 0L) {
      return Array(Oracle11InputPartition(None))
    }

    val stride = diff / numPartitions
    if (stride <= 0L) {
      return Array(Oracle11InputPartition(None))
    }

    val boundaries = (1 until numPartitions).map { i =>
      new Date(lowerMs + stride * i)
    }

    buildPartitions(columnSql, boundaries, DateType)
  }

  private def planTimestamp(
      columnSql: String,
      lower: Timestamp,
      upper: Timestamp,
      numPartitions: Int): Array[Oracle11InputPartition] = {

    val lowerMs = lower.getTime
    val upperMs = upper.getTime
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
