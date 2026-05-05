package com.company.spark.oracle11

import java.math.{BigDecimal => JBigDecimal}
import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate, LocalDateTime, OffsetDateTime}
import java.nio.charset.StandardCharsets

import com.company.spark.oracle.oci.{OciConnection, OciRow, OciStatement}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

object Oracle11OciUtils {

  def openConnection(options: Oracle11Options): OciConnection =
    OciConnection.connect(
      connectString = options.connectString,
      user = options.user,
      password = options.password,
      prefetchRows = options.fetchSize
    )

  def closeQuietly(resource: AutoCloseable): Unit = {
    if (resource != null) {
      try {
        resource.close()
      } catch {
        case _: Throwable =>
      }
    }
  }

  def bindParameters(statement: OciStatement, params: Seq[JdbcParameter]): Unit = {
    params.zipWithIndex.foreach { case (param, i) =>
      bindParameter(statement, i + 1, param)
    }
  }

  def bindParameter(statement: OciStatement, index: Int, param: JdbcParameter): Unit = {
    val value = param.value
    if (value == null) {
      statement.bindString(index, null)
      return
    }

    param.sparkType match {
      case IntegerType | ShortType | ByteType =>
        statement.bindInt(index, toInt(value))
      case LongType =>
        statement.bindLong(index, toLong(value))
      case FloatType | DoubleType =>
        statement.bindDouble(index, toDouble(value))
      case BooleanType =>
        statement.bindInt(index, if (toBoolean(value)) 1 else 0)
      case StringType =>
        statement.bindString(index, value.toString)
      case _: DecimalType =>
        statement.bindNumber(index, BigDecimal(toJavaBigDecimal(value)))
      case TimestampType =>
        statement.bindTimestamp(index, toTimestamp(value))
      case DateType =>
        statement.bindDate(index, toDate(value))
      case BinaryType =>
        throw new IllegalArgumentException("Binary bind parameters are not supported in native OCI mode")
      case _ =>
        statement.bindString(index, value.toString)
    }
  }

  // Reuses the same row object between `next()` calls to minimize per-row allocations.
  def buildReusableInternalRowExtractor(schema: StructType): OciRow => InternalRow = {
    if (schema.isEmpty) {
      (_: OciRow) => InternalRow.empty
    } else {
      val readers = schema.fields.zipWithIndex.map { case (field, idx) =>
        buildColumnReader(field.dataType, idx)
      }.toArray

      val values = new Array[Any](readers.length)
      val row = new GenericInternalRow(values)
      (ociRow: OciRow) => {
        var i = 0
        while (i < readers.length) {
          values(i) = readers(i)(ociRow)
          i += 1
        }
        row
      }
    }
  }

  private def buildColumnReader(dataType: DataType, index: Int): OciRow => Any = dataType match {
    case IntegerType =>
      (row: OciRow) => nullable(row, index, v => toInt(v))
    case LongType =>
      (row: OciRow) => nullable(row, index, v => toLong(v))
    case DoubleType =>
      (row: OciRow) => nullable(row, index, v => toDouble(v))
    case FloatType =>
      (row: OciRow) => nullable(row, index, v => toDouble(v).toFloat)
    case ShortType =>
      (row: OciRow) => nullable(row, index, v => toInt(v).toShort)
    case ByteType =>
      (row: OciRow) => nullable(row, index, v => toInt(v).toByte)
    case BooleanType =>
      (row: OciRow) => nullable(row, index, v => toBoolean(v))
    case StringType =>
      (row: OciRow) => nullable(row, index, v => UTF8String.fromString(v.toString))
    case BinaryType =>
      (row: OciRow) => nullable(row, index, toBinary)
    case t: DecimalType =>
      (row: OciRow) => nullable(row, index, v => Oracle11JdbcUtils.toSparkDecimal(toJavaBigDecimal(v), t))
    case TimestampType =>
      (row: OciRow) => nullable(row, index, v => DateTimeUtils.fromJavaTimestamp(toTimestamp(v)))
    case DateType =>
      (row: OciRow) => nullable(row, index, v => DateTimeUtils.fromJavaDate(toDate(v)))
    case _ =>
      (row: OciRow) => nullable(row, index, v => UTF8String.fromString(v.toString))
  }

  private def nullable(row: OciRow, index: Int, convert: Any => Any): Any = {
    val value = row.get(index)
    if (value == null) null else convert(value)
  }

  private def toBinary(value: Any): Array[Byte] = value match {
    case b: Array[Byte] => b
    case other          => other.toString.getBytes(StandardCharsets.UTF_8)
  }

  private def toInt(value: Any): Int = value match {
    case n: java.lang.Number => n.intValue()
    case s: String           => s.toInt
    case other               => throw new IllegalArgumentException(s"Cannot cast '$other' to Int")
  }

  private def toLong(value: Any): Long = value match {
    case n: java.lang.Number => n.longValue()
    case s: String           => s.toLong
    case ts: Timestamp       => ts.getTime
    case d: Date             => d.getTime
    case other               => throw new IllegalArgumentException(s"Cannot cast '$other' to Long")
  }

  private def toDouble(value: Any): Double = value match {
    case n: java.lang.Number => n.doubleValue()
    case s: String           => s.toDouble
    case other               => throw new IllegalArgumentException(s"Cannot cast '$other' to Double")
  }

  private def toBoolean(value: Any): Boolean = value match {
    case b: java.lang.Boolean => b.booleanValue()
    case s: String            => s.toBoolean
    case n: java.lang.Number  => n.intValue() != 0
    case other                => throw new IllegalArgumentException(s"Cannot cast '$other' to Boolean")
  }

  private def toJavaBigDecimal(value: Any): JBigDecimal = value match {
    case d: Decimal                => d.toJavaBigDecimal
    case bd: JBigDecimal           => bd
    case bd: scala.math.BigDecimal => bd.bigDecimal
    case n: java.lang.Number       => new JBigDecimal(n.toString)
    case s: String                 => new JBigDecimal(s)
    case other                     => throw new IllegalArgumentException(s"Cannot cast '$other' to BigDecimal")
  }

  private def toTimestamp(value: Any): Timestamp = value match {
    case ts: Timestamp       => ts
    case d: java.util.Date   => new Timestamp(d.getTime)
    case i: Instant          => Timestamp.from(i)
    case odt: OffsetDateTime => Timestamp.from(odt.toInstant)
    case ldt: LocalDateTime  => Timestamp.valueOf(ldt)
    case ld: LocalDate       => Timestamp.valueOf(ld.atStartOfDay())
    case s: String           => Oracle11JdbcUtils.parseTimestampBound(s)
    case l: java.lang.Long   => new Timestamp(l)
    case n: java.lang.Number => new Timestamp(n.longValue())
    case other               => throw new IllegalArgumentException(s"Cannot cast '$other' to Timestamp")
  }

  private def toDate(value: Any): Date = value match {
    case d: Date             => d
    case ld: LocalDate       => Date.valueOf(ld)
    case ldt: LocalDateTime  => Date.valueOf(ldt.toLocalDate)
    case ts: Timestamp       => new Date(ts.getTime)
    case s: String           => Oracle11JdbcUtils.parseDateBound(s)
    case other               => throw new IllegalArgumentException(s"Cannot cast '$other' to Date")
  }
}
