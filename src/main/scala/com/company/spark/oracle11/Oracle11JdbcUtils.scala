package com.company.spark.oracle11

import java.math.{BigDecimal => JBigDecimal}
import java.sql.{Connection, Date, DriverManager, PreparedStatement, ResultSet, SQLException, Timestamp, Types}
import java.time.{Instant, LocalDate, LocalDateTime, OffsetDateTime}
import java.util.{Locale, Properties}

import scala.util.Try

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

object Oracle11JdbcUtils {
  private type ColumnReader = (ResultSet, Int) => Any
  private val localeRetryLock = new Object

  def openConnection(options: Oracle11Options): Connection = {
    val props = new Properties()
    props.setProperty("user", options.user)
    props.setProperty("password", options.password)
    props.setProperty("defaultRowPrefetch", options.fetchSize.toString)
    options.connectTimeoutMs.foreach(v => props.setProperty("oracle.net.CONNECT_TIMEOUT", v.toString))
    options.readTimeoutMs.foreach(v => props.setProperty("oracle.jdbc.ReadTimeout", v.toString))

    try {
      DriverManager.getConnection(options.url, props)
    } catch {
      // Some Oracle setups fail logon with ORA-12705 unless JVM locale is English/US.
      // We retry once with temporary locale override instead of requiring manual Spark JVM flags.
      case first: SQLException if containsOra12705(first) =>
        openConnectionWithEnglishLocaleRetry(options.url, props, first)
    }
  }

  private def openConnectionWithEnglishLocaleRetry(
      url: String,
      props: Properties,
      original: SQLException): Connection = {
    localeRetryLock.synchronized {
      val previousLocale = Locale.getDefault
      val previousLanguage = System.getProperty("user.language")
      val previousCountry = System.getProperty("user.country")

      try {
        Locale.setDefault(Locale.US)
        System.setProperty("user.language", "en")
        System.setProperty("user.country", "US")
        DriverManager.getConnection(url, props)
      } catch {
        case retryErr: SQLException =>
          retryErr.addSuppressed(original)
          throw retryErr
      } finally {
        Locale.setDefault(previousLocale)
        restoreSystemProperty("user.language", previousLanguage)
        restoreSystemProperty("user.country", previousCountry)
      }
    }
  }

  private def restoreSystemProperty(name: String, value: String): Unit = {
    if (value == null) {
      System.clearProperty(name)
    } else {
      System.setProperty(name, value)
    }
  }

  private def containsOra12705(error: Throwable): Boolean = {
    var current = error
    while (current != null) {
      val message = Option(current.getMessage).getOrElse("")
      if (message.contains("ORA-12705")) {
        return true
      }
      current = current.getCause
    }
    false
  }

  def quoteIdentifier(name: String): String =
    "\"" + name.replace("\"", "\"\"") + "\""

  def bindParameters(statement: PreparedStatement, params: Seq[JdbcParameter]): Unit = {
    params.zipWithIndex.foreach { case (param, idx) =>
      bindParameter(statement, idx + 1, param)
    }
  }

  def bindParameter(statement: PreparedStatement, index: Int, param: JdbcParameter): Unit = {
    val value = param.value
    if (value == null) {
      statement.setNull(index, sqlNullType(param.sparkType))
      return
    }

    param.sparkType match {
      case IntegerType => statement.setInt(index, toInt(value))
      case LongType    => statement.setLong(index, toLong(value))
      case DoubleType  => statement.setDouble(index, toDouble(value))
      case FloatType   => statement.setFloat(index, toFloat(value))
      case ShortType   => statement.setShort(index, toShort(value))
      case ByteType    => statement.setByte(index, toByte(value))
      case BooleanType => statement.setBoolean(index, toBoolean(value))
      case StringType  => statement.setString(index, value.toString)
      case BinaryType  => statement.setBytes(index, value.asInstanceOf[Array[Byte]])
      case _: DecimalType =>
        statement.setBigDecimal(index, toJavaBigDecimal(value))
      case TimestampType =>
        statement.setTimestamp(index, toTimestamp(value))
      case DateType =>
        statement.setDate(index, toDate(value))
      case _ =>
        statement.setObject(index, value)
    }
  }

  def normalizeLiteral(value: Any, expectedType: DataType): Option[Any] = {
    if (value == null) {
      return Some(null)
    }

    Try {
      expectedType match {
        case IntegerType    => toInt(value)
        case LongType       => toLong(value)
        case DoubleType     => toDouble(value)
        case FloatType      => toFloat(value)
        case ShortType      => toShort(value)
        case ByteType       => toByte(value)
        case BooleanType    => toBoolean(value)
        case StringType     => value.toString
        case BinaryType     => value.asInstanceOf[Array[Byte]]
        case _: DecimalType => toJavaBigDecimal(value)
        case TimestampType  => toTimestamp(value)
        case DateType       => toDate(value)
        case _              => value
      }
    }.toOption
  }

  def buildInternalRowExtractor(schema: StructType): ResultSet => InternalRow = {
    if (schema.isEmpty) {
      (_: ResultSet) => InternalRow.empty
    } else {
      val readers = buildReaders(schema)
      (resultSet: ResultSet) => {
        val values = new Array[Any](readers.length)
        var i = 0
        while (i < readers.length) {
          values(i) = readers(i)(resultSet, i + 1)
          i += 1
        }
        new GenericInternalRow(values)
      }
    }
  }

  // Reuses the same row object between `next()` calls to reduce per-row allocations on scan hot path.
  def buildReusableInternalRowExtractor(schema: StructType): ResultSet => InternalRow = {
    if (schema.isEmpty) {
      (_: ResultSet) => InternalRow.empty
    } else {
      val readers = buildReaders(schema)
      val values = new Array[Any](readers.length)
      val row = new GenericInternalRow(values)
      (resultSet: ResultSet) => {
        var i = 0
        while (i < readers.length) {
          values(i) = readers(i)(resultSet, i + 1)
          i += 1
        }
        row
      }
    }
  }

  def toInternalRow(resultSet: ResultSet, schema: StructType): InternalRow =
    buildInternalRowExtractor(schema)(resultSet)

  private def buildReaders(schema: StructType): Array[ColumnReader] =
    schema.fields.map(f => buildColumnReader(f.dataType)).toArray

  private def buildColumnReader(dataType: DataType): ColumnReader = dataType match {
    case IntegerType =>
      (rs, idx) => {
        val v = rs.getInt(idx)
        if (rs.wasNull()) null else v
      }

    case LongType =>
      (rs, idx) => {
        val v = rs.getLong(idx)
        if (rs.wasNull()) null else v
      }

    case DoubleType =>
      (rs, idx) => {
        val v = rs.getDouble(idx)
        if (rs.wasNull()) null else v
      }

    case FloatType =>
      (rs, idx) => {
        val v = rs.getFloat(idx)
        if (rs.wasNull()) null else v
      }

    case ShortType =>
      (rs, idx) => {
        val v = rs.getShort(idx)
        if (rs.wasNull()) null else v
      }

    case ByteType =>
      (rs, idx) => {
        val v = rs.getByte(idx)
        if (rs.wasNull()) null else v
      }

    case BooleanType =>
      (rs, idx) => {
        val v = rs.getBoolean(idx)
        if (rs.wasNull()) null else v
      }

    case StringType =>
      (rs, idx) => {
        val v = rs.getString(idx)
        if (v == null) null else UTF8String.fromString(v)
      }

    case BinaryType =>
      (rs, idx) => rs.getBytes(idx)

    case t: DecimalType =>
      (rs, idx) => {
        val v = rs.getBigDecimal(idx)
        if (v == null) {
          null
        } else {
          toSparkDecimal(v, t)
        }
      }

    case TimestampType =>
      (rs, idx) => {
        val ts = rs.getTimestamp(idx)
        if (ts == null) null else DateTimeUtils.fromJavaTimestamp(ts)
      }

    case DateType =>
      (rs, idx) => {
        val d = rs.getDate(idx)
        if (d == null) null else DateTimeUtils.fromJavaDate(d)
      }

    case _ =>
      (rs, idx) => {
        val v = rs.getObject(idx)
        if (v == null) null else UTF8String.fromString(v.toString)
      }
  }

  private[oracle11] def toSparkDecimal(value: JBigDecimal, targetType: DecimalType): Decimal = {
    if (value == null) {
      return null
    }

    // Fast path avoids exception-driven control flow for out-of-range values.
    if (fitsInDecimalType(value, targetType)) {
      try {
        Decimal(value, targetType.precision, targetType.scale)
      } catch {
        case _: ArithmeticException => Decimal(value)
      }
    } else {
      Decimal(value)
    }
  }

  private def fitsInDecimalType(value: JBigDecimal, targetType: DecimalType): Boolean = {
    val precision = value.precision()
    val scale = value.scale()
    precision <= targetType.precision && scale >= 0 && scale <= targetType.scale
  }

  def closeQuietly(resource: AutoCloseable): Unit = {
    if (resource != null) {
      try {
        resource.close()
      } catch {
        case _: Throwable =>
      }
    }
  }

  private def sqlNullType(dataType: DataType): Int = dataType match {
    case IntegerType    => Types.INTEGER
    case LongType       => Types.BIGINT
    case DoubleType     => Types.DOUBLE
    case FloatType      => Types.FLOAT
    case ShortType      => Types.SMALLINT
    case ByteType       => Types.TINYINT
    case BooleanType    => Types.BOOLEAN
    case StringType     => Types.VARCHAR
    case BinaryType     => Types.BINARY
    case _: DecimalType => Types.DECIMAL
    case TimestampType  => Types.TIMESTAMP
    case DateType       => Types.DATE
    case _              => Types.JAVA_OBJECT
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
    case other               => throw new IllegalArgumentException(s"Cannot cast '$other' to Long")
  }

  private def toDouble(value: Any): Double = value match {
    case n: java.lang.Number => n.doubleValue()
    case s: String           => s.toDouble
    case other               => throw new IllegalArgumentException(s"Cannot cast '$other' to Double")
  }

  private def toFloat(value: Any): Float = value match {
    case n: java.lang.Number => n.floatValue()
    case s: String           => s.toFloat
    case other               => throw new IllegalArgumentException(s"Cannot cast '$other' to Float")
  }

  private def toShort(value: Any): Short = value match {
    case n: java.lang.Number => n.shortValue()
    case s: String           => s.toShort
    case other               => throw new IllegalArgumentException(s"Cannot cast '$other' to Short")
  }

  private def toByte(value: Any): Byte = value match {
    case n: java.lang.Number => n.byteValue()
    case s: String           => s.toByte
    case other               => throw new IllegalArgumentException(s"Cannot cast '$other' to Byte")
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
    case s: String           => parseTimestamp(s)
    case l: java.lang.Long   => new Timestamp(l)
    case n: java.lang.Number => new Timestamp(n.longValue())
    case other               => throw new IllegalArgumentException(s"Cannot cast '$other' to Timestamp")
  }

  private def toDate(value: Any): Date = value match {
    case d: Date             => d
    case ld: LocalDate       => Date.valueOf(ld)
    case ldt: LocalDateTime  => Date.valueOf(ldt.toLocalDate)
    case ts: Timestamp       => new Date(ts.getTime)
    case s: String           => parseDate(s)
    case other               => throw new IllegalArgumentException(s"Cannot cast '$other' to Date")
  }

  def parseTimestampBound(raw: String): Timestamp = parseTimestamp(raw)

  def parseDateBound(raw: String): Date = parseDate(raw)

  private def parseTimestamp(raw: String): Timestamp = {
    val value = raw.trim

    val attempts: Seq[() => Timestamp] = Seq(
      () => Timestamp.from(Instant.parse(value)),
      () => Timestamp.from(OffsetDateTime.parse(value).toInstant),
      () => Timestamp.valueOf(value),
      () => Timestamp.valueOf(value.replace('T', ' ')),
      () => Timestamp.valueOf(LocalDateTime.parse(value)),
      () => Timestamp.valueOf(LocalDate.parse(value).atStartOfDay())
    )

    attempts.iterator
      .map(parser => Try(parser()))
      .collectFirst { case scala.util.Success(ts) => ts }
      .getOrElse {
        throw new IllegalArgumentException(s"Cannot parse timestamp value '$raw'")
      }
  }

  private def parseDate(raw: String): Date = {
    val value = raw.trim
    val attempts: Seq[() => Date] = Seq(
      () => Date.valueOf(LocalDate.parse(value)),
      () => new Date(parseTimestamp(value).getTime)
    )

    attempts.iterator
      .map(parser => Try(parser()))
      .collectFirst { case scala.util.Success(date) => date }
      .getOrElse {
        throw new IllegalArgumentException(s"Cannot parse date value '$raw'")
      }
  }
}
