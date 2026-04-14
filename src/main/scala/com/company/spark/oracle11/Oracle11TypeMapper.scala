package com.company.spark.oracle11

import java.sql.Types
import java.util.Locale

import org.apache.spark.sql.types._

object Oracle11TypeMapper {

  def toSparkType(jdbcType: Int, precision: Int, scale: Int, typeName: String): DataType = {
    val normalized = Option(typeName).map(_.toUpperCase(Locale.ROOT)).getOrElse("")

    normalized match {
      case "VARCHAR2" | "VARCHAR" | "CHAR" | "NCHAR" | "NVARCHAR2" =>
        return StringType
      case "CLOB" | "NCLOB" =>
        return StringType
      case "BLOB" | "RAW" | "LONG RAW" =>
        return BinaryType
      case "DATE" | "TIMESTAMP" | "TIMESTAMP WITH TIME ZONE" | "TIMESTAMP WITH LOCAL TIME ZONE" =>
        return TimestampType
      case "FLOAT" =>
        return DoubleType
      case _ =>
    }

    jdbcType match {
      case Types.VARCHAR | Types.LONGVARCHAR | Types.CHAR | Types.NVARCHAR | Types.NCHAR | Types.LONGNVARCHAR =>
        StringType
      case Types.CLOB | Types.NCLOB =>
        StringType
      case Types.BINARY | Types.VARBINARY | Types.LONGVARBINARY | Types.BLOB =>
        BinaryType
      case Types.DATE | Types.TIMESTAMP | Types.TIMESTAMP_WITH_TIMEZONE =>
        TimestampType
      case Types.FLOAT | Types.REAL | Types.DOUBLE =>
        DoubleType
      case Types.INTEGER | Types.SMALLINT | Types.TINYINT =>
        IntegerType
      case Types.BIGINT =>
        LongType
      case Types.NUMERIC | Types.DECIMAL =>
        mapNumber(precision, scale)
      case _ =>
        StringType
    }
  }

  private def mapNumber(precision: Int, scale: Int): DataType = {
    if (scale == 0) {
      if (precision >= 1 && precision <= 9) {
        IntegerType
      } else if (precision >= 10 && precision <= 18) {
        LongType
      } else if (precision >= 19 && precision <= 38) {
        DecimalType(precision, 0)
      } else {
        DecimalType(38, 0)
      }
    } else if (scale > 0) {
      if (precision > 0 && precision <= 38 && scale <= 38 && scale <= precision) {
        DecimalType(precision, scale)
      } else {
        DoubleType
      }
    } else {
      val computedPrecision = math.min(38, math.max(1, precision - scale))
      if (computedPrecision >= 1 && computedPrecision <= 38) {
        DecimalType(computedPrecision, 0)
      } else {
        DoubleType
      }
    }
  }
}
