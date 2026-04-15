package com.company.spark.oracle11

import java.math.MathContext
import java.sql.{Date, Timestamp}
import java.util.logging.Logger

import scala.util.control.NonFatal

import org.apache.spark.sql.types._

object Oracle11PartitionPlanner {
  private val logger = Logger.getLogger(getClass.getName)

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
          try {
            planWithAutoBounds(
              options = options,
              relation = relation,
              field = field,
              columnSql = columnSql,
              requestedPartitions = partitioning.numPartitions,
              pushedPredicate = pushedPredicate
            )
          } catch {
            case NonFatal(err) =>
              logger.warning(
                s"Auto partition bounds failed for column '${field.name}'. Falling back to single partition. ${err.getMessage}")
              Array(Oracle11InputPartition(None))
          }
        } else {
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
  }

  private def planWithAutoBounds(
      options: Oracle11Options,
      relation: Oracle11Relation,
      field: StructField,
      columnSql: String,
      requestedPartitions: Int,
      pushedPredicate: Option[Oracle11SqlPredicate]): Array[Oracle11InputPartition] = {

    val statsSql = {
      val where = pushedPredicate.map(p => s" WHERE (${p.sql})").getOrElse("")
      s"SELECT MIN($columnSql), MAX($columnSql), COUNT(1) FROM ${relation.fromClause}$where"
    }

    val conn = Oracle11JdbcUtils.openConnection(options)
    var stmt: java.sql.PreparedStatement = null
    var rs: java.sql.ResultSet = null

    try {
      safely {
        conn.setReadOnly(true)
      }
      safely {
        conn.setAutoCommit(false)
      }

      stmt = conn.prepareStatement(statsSql)
      options.queryTimeoutSec.foreach(stmt.setQueryTimeout)
      pushedPredicate.foreach(p => Oracle11JdbcUtils.bindParameters(stmt, p.params))
      rs = stmt.executeQuery()

      if (!rs.next()) {
        return Array(Oracle11InputPartition(None))
      }

      val count = rs.getLong(3)
      val effectivePartitions = computeEffectivePartitions(
        requested = requestedPartitions,
        rowCount = count,
        minRowsPerPartition = options.autoPartitionMinRowsPerPartition
      )

      if (effectivePartitions <= 1) {
        return Array(Oracle11InputPartition(None))
      }

      field.dataType match {
        case dt if isNumeric(dt) =>
          val lower = rs.getBigDecimal(1)
          val upper = rs.getBigDecimal(2)
          if (lower == null || upper == null) {
            Array(Oracle11InputPartition(None))
          } else {
            planNumeric(columnSql, BigDecimal(lower), BigDecimal(upper), effectivePartitions)
          }

        case DateType =>
          val lower = rs.getDate(1)
          val upper = rs.getDate(2)
          if (lower == null || upper == null) {
            Array(Oracle11InputPartition(None))
          } else {
            planDate(columnSql, lower, upper, effectivePartitions)
          }

        case TimestampType =>
          val lower = rs.getTimestamp(1)
          val upper = rs.getTimestamp(2)
          if (lower == null || upper == null) {
            Array(Oracle11InputPartition(None))
          } else {
            planTimestamp(columnSql, lower, upper, effectivePartitions)
          }

        case other =>
          throw new IllegalArgumentException(
            s"Partition column '${field.name}' has unsupported type '$other'. Only numeric/date/timestamp are supported")
      }
    } finally {
      Oracle11JdbcUtils.closeQuietly(rs)
      Oracle11JdbcUtils.closeQuietly(stmt)
      Oracle11JdbcUtils.closeQuietly(conn)
    }
  }

  private[oracle11] def computeEffectivePartitions(
      requested: Int,
      rowCount: Long,
      minRowsPerPartition: Long): Int = {
    val rowBased =
      if (rowCount <= 0L) {
        1
      } else {
        val n = math.ceil(rowCount.toDouble / minRowsPerPartition.toDouble).toInt
        math.max(1, n)
      }

    math.max(1, math.min(requested, rowBased))
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
        planNumeric(columnSql, lower, upper, numPartitions)

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
      numPartitions: Int): Array[Oracle11InputPartition] = {

    val diff = upper - lower
    if (diff <= 0) {
      return Array(Oracle11InputPartition(None))
    }

    val stride = BigDecimal(
      diff.bigDecimal.divide(BigDecimal(numPartitions).bigDecimal, MathContext.DECIMAL128))
    if (stride <= 0) {
      return Array(Oracle11InputPartition(None))
    }

    val boundaries = (1 until numPartitions).map(i => (lower + stride * i).bigDecimal)
    buildPartitions(columnSql, boundaries, DecimalType(38, 18))
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

  private def safely(op: => Unit): Unit = {
    try {
      op
    } catch {
      case NonFatal(_) =>
    }
  }
}
